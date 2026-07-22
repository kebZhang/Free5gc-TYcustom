package nas

import (
	"fmt"

	"github.com/free5gc/amf/internal/accesslog"
	amf_context "github.com/free5gc/amf/internal/context"
	gmm_common "github.com/free5gc/amf/internal/gmm/common"
	"github.com/free5gc/amf/internal/logger"
	"github.com/free5gc/amf/internal/msgtrace"
	"github.com/free5gc/amf/internal/nas/nas_security"
	"github.com/free5gc/amf/internal/recvtime"
	"github.com/free5gc/nas"
	"github.com/free5gc/nas/nasConvert"
	"github.com/free5gc/nas/nasMessage"
	nas_metrics "github.com/free5gc/util/metrics/nas"
)

func HandleNAS(ranUe *amf_context.RanUe, procedureCode int64, nasPdu []byte, initialMessage bool) {
	isNasMsgRcv := false
	metricCause := ""
	nasMsg := nas.NewMessage()
	// The closure here is for not having to add a deep copy func for the nas.Message type.
	defer func() {
		nas_metrics.IncrMetricsRcvNasMsg(nasMsg, &isNasMsgRcv, &metricCause)
	}()

	amfSelf := amf_context.GetSelf()

	if ranUe == nil {
		metricCause = nas_metrics.RAN_UE_NIL_ERR
		logger.NasLog.Error("RanUe is nil")
		return
	}

	if nasPdu == nil {
		metricCause = nas_metrics.NAS_PDU_NIL_ERR
		ranUe.Log.Error("nasPdu is nil")
		return
	}

	if ranUe.AmfUe == nil {
		// Only the New created RanUE will have no AmfUe in it

		if ranUe.HoldingAmfUe != nil && !ranUe.HoldingAmfUe.CmConnect(ranUe.Ran.AnType) {
			// If the UE is CM-IDLE, there is no RanUE in AmfUe, so here we attach new RanUe to AmfUe.
			gmm_common.AttachRanUeToAmfUeAndReleaseOldIfAny(ranUe.HoldingAmfUe, ranUe)
			ranUe.HoldingAmfUe = nil
		} else {
			// Assume we have an existing UE context in CM-CONNECTED state. (RanUe <-> AmfUe)
			// We will release it if the new UE context has a valid security context(Authenticated) in line 50.
			ranUe.AmfUe = amfSelf.NewAmfUe("")
			gmm_common.AttachRanUeToAmfUeAndReleaseOldIfAny(ranUe.AmfUe, ranUe)
		}
	}

	msg, integrityProtected, err := nas_security.Decode(ranUe.AmfUe, ranUe.Ran.AnType, nasPdu, initialMessage)
	if err != nil {
		metricCause = nas_metrics.DECODE_NAS_MSG_ERR
		ranUe.AmfUe.NASLog.Errorln(err)
		return
	}

	nasMsg = msg

	// AMF_log: record the SCTP-read time for the uplink NAS messages of interest.
	// The read time was captured at SCTPRead and carried (goroutine-local) to here;
	// logging is asynchronous and never blocks this path.
	logUplinkNAS(ranUe, msg)

	ranUe.AmfUe.NasPduValue = nasPdu
	ranUe.AmfUe.MacFailed = !integrityProtected

	if ranUe.AmfUe.SecurityContextIsValid() && ranUe.HoldingAmfUe != nil {
		gmm_common.ClearHoldingRanUe(ranUe.HoldingAmfUe.RanUe[ranUe.Ran.AnType])
		ranUe.HoldingAmfUe = nil
	}

	isNasMsgRcv = true

	if errDispatch := Dispatch(ranUe.AmfUe, ranUe.Ran.AnType, procedureCode, msg); errDispatch != nil {
		ranUe.AmfUe.NASLog.Errorf("Handle NAS Error: %v", errDispatch)
		isNasMsgRcv = false
	}
}

// logUplinkNAS asynchronously records the SCTP-read time of the uplink NAS
// messages we care about (Registration Request, Authentication Response,
// Security Mode Complete) to AMF_log. It is the uplink counterpart of the
// downlink logging in the ngap/message send path.
//
// The read time was captured at SCTPRead and carried goroutine-locally to here.
// All work is cheap (a type switch + an async enqueue); if no read time is
// present (e.g. unexpected call path) the message is simply skipped.
func logUplinkNAS(ranUe *amf_context.RanUe, msg *nas.Message) {
	if msg == nil || msg.GmmMessage == nil {
		return
	}

	var nasType string
	switch msg.GmmMessage.GmmHeader.GetMessageType() {
	case nas.MsgTypeRegistrationRequest:
		nasType = "RegistrationRequest"
	case nas.MsgTypeAuthenticationResponse:
		nasType = "AuthenticationResponse"
	case nas.MsgTypeSecurityModeComplete:
		nasType = "SecurityModeComplete"
	default:
		return // not a message of interest
	}

	t, ok := recvtime.Current()
	if !ok {
		return
	}

	var ueID string
	if amfUe := ranUe.AmfUe; amfUe != nil {
		if amfUe.Supi != "" {
			ueID = amfUe.Supi
		} else {
			ueID = amfUe.Suci
		}
	}

	// For the very first uplink message (RegistrationRequest) the AmfUe has not
	// been populated yet (Suci/Supi are still empty), so ueID would otherwise be
	// "". The SUCI is, however, already present in the RegistrationRequest body,
	// so derive it here purely for logging. This is read-only on the NAS message,
	// never touches ranUe/amfUe state, and any failure is silently ignored — it
	// cannot affect the registration business logic.
	if ueID == "" && nasType == "RegistrationRequest" {
		if suci, ok := suciFromRegistrationRequest(msg); ok {
			ueID = suci
		}
	}

	accesslog.LogNGAP("UL", nasType, ueID, t)

	// AMF_worker_log: fill in the ue_id / nas_type of the worker trace started
	// for this message (they are only known now, after NAS decode). The SBI
	// calls this message triggers are appended later on the same goroutine.
	msgtrace.SetID(ueID, nasType)
}

// suciFromRegistrationRequest extracts the SUCI string (e.g.
// "suci-0-999-70-0-0-0-0000000004") from a RegistrationRequest, using the same
// decoding as the GMM handler so the log format matches AmfUe.Suci exactly.
//
// It is logging-only and intentionally defensive: it reads from the decoded NAS
// message without mutating any context, returns ("", false) on anything
// unexpected, and recovers from any panic in the underlying decoder so it can
// never impact the data path. Only the SUCI identity type yields a value; GUTI /
// no-identity registrations return ("", false) and keep ue_id empty as before.
func suciFromRegistrationRequest(msg *nas.Message) (suci string, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			suci, ok = "", false
		}
	}()

	if msg == nil || msg.GmmMessage == nil || msg.RegistrationRequest == nil {
		return "", false
	}

	contents := msg.RegistrationRequest.MobileIdentity5GS.GetMobileIdentity5GSContents()
	if len(contents) < 1 {
		return "", false
	}
	if nasConvert.GetTypeOfIdentity(contents[0]) != nasMessage.MobileIdentity5GSTypeSuci {
		return "", false
	}

	s, plmnId, err := nasConvert.SuciToStringWithError(contents)
	if err != nil || plmnId == "" || s == "" {
		return "", false
	}
	return s, true
}

// Get5GSMobileIdentityFromNASPDU is used to find MobileIdentity from plain nas
// return value is: mobileId, mobileIdType, err
func GetNas5GSMobileIdentity(gmmMessage *nas.GmmMessage) (string, string, error) {
	var err error
	var mobileId, mobileIdType string

	if gmmMessage.GmmHeader.GetMessageType() == nas.MsgTypeRegistrationRequest {
		mobileId, mobileIdType, err = gmmMessage.RegistrationRequest.MobileIdentity5GS.GetMobileIdentity()
	} else if gmmMessage.GmmHeader.GetMessageType() == nas.MsgTypeServiceRequest {
		mobileId, mobileIdType, err = gmmMessage.ServiceRequest.TMSI5GS.Get5GSTMSI()
	} else {
		err = fmt.Errorf("gmmMessageType: [%d] is not RegistrationRequest or ServiceRequest",
			gmmMessage.GmmHeader.GetMessageType())
	}
	return mobileId, mobileIdType, err
}
