// SPDX-FileCopyrightText: 2022 Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	ctxt "context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/omec-project/nas"
	"github.com/omec-project/nas/nasConvert"
	"github.com/omec-project/nas/nasMessage"
	"github.com/omec-project/ngap"
	"github.com/omec-project/ngap/ngapType"
	"github.com/omec-project/sctplb/context"
	"github.com/omec-project/sctplb/logger"
	gClient "github.com/omec-project/sctplb/sdcoreAmfServer"
	"github.com/omec-project/util/drsm"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

type backendNF struct {
	address string
	conn    *grpc.ClientConn
	gc      gClient.NgapServiceClient
	state   bool
	stream  gClient.NgapService_HandleMessageClient
	count   int64
}

var (
	// backends      []*backendNF
	backends      map[string]*backendNF //map[*backendNF]int64
	nfNum         int64
	mutex         sync.RWMutex
	drsmInitDone  bool = false
	Drsm          drsm.DrsmInterface
	redirectCount int32
)

func InitDrsm() (drsm.DrsmInterface, error) {
	podname := os.Getenv("HOSTNAME")
	podip := os.Getenv("POD_IP")
	podId := drsm.PodId{PodName: podname, PodIp: podip}
	dbUrl := "mongodb://mongodb-arbiter-headless"
	// dbUrl := "mongodb://mongodb1-arbiter-headless"
	opt := &drsm.Options{ResIdSize: 24, Mode: drsm.ResourceDemux}
	db := drsm.DbInfo{Url: dbUrl, Name: "sdcore_amf"}

	return drsm.InitDRSM("amfid", podId, db, opt)
}

// returns the backendNF using RoundRobin algorithm
func MinLogic(amfId int64) (nf *backendNF) {
	// logger.ConsumerLog.Warnln("RoundRobin mutex.Lock()")
	mutex.Lock()
	defer mutex.Unlock()
	// defer logger.ConsumerLog.Warnln("RoundRobin mutex.Unlock()")

	if len(backends) <= 0 {
		logger.DispatchLog.Errorln("There are no backend NFs running")
		return nil
	}

	if amfId != 0 && drsmInitDone {
		var id *drsm.PodId
		// logger.DispatchLog.Errorln("find amfid owner ", amfId)
		id, _ = Drsm.FindOwnerInt32ID(int32(amfId))
		if id != nil {
			// logger.DispatchLog.Infof(" AMF found for amfId: %v id: %v", amfId, id)
			if backend, ok := backends[id.PodIp]; ok {
				if backend.state == true {
					return backend
				}
			}
			// for b1, _ := range backends {
			// 	if b1.state == true && b1.address == id.PodIp {
			// 		// logger.DispatchLog.Infof(" 2 found owner ", b1.address)
			// 		return b1
			// 	}
			// }
		} else {
			logger.DispatchLog.Infof(" AMF not found for amfId: %v ", amfId)
		}
		//  else {
		// 	index := amfId % nfNum
		// 	if backends[index].state == true {
		// 		// logger.DiscoveryLog.Infof("Returning same AMF for: AmfID %v backend %v, index %v", amfId, backends[index].address, index)
		// 		return backends[index]
		// 	}
		// 	// logger.DiscoveryLog.Infof("AMF state id Down for: AmfID %v backend %v, next %v", amfId, backends[index].address, index)
		// }
	}

	min := int64(math.MaxInt64)
	for _, backend := range backends {
		if min > backend.count {
			nf = backend
			min = backend.count
		}
	}
	if amfId == 0 { // Bilal: check if this is necessery or not
		nf.count++
	}

	// nf = backends[next]
	// next++
	// if next >= nfNum {
	// 	next = 0
	// }

	logger.DiscoveryLog.Infof("Default round Robin: AmfID %+v backend %+v, next %v", amfId, nf.address, min)
	return nf
}

func dispatchAddServer(serviceName string) {
	// add server in pool
	// create server
	// create server outstanding message queue
	// connect to server
	// there can be more than 1 message outstanding toards same server

	if drsmInitDone == false {
		Drsm, _ = InitDrsm()
		drsmInitDone = true
	}
	// backends = make(map[*backendNF]int64)
	backends = make(map[string]*backendNF)

	for {
		logger.DiscoveryLog.Traceln("Discover Service ", serviceName)
		ips, err := net.LookupIP(serviceName)
		if err != nil {
			logger.DiscoveryLog.Errorln("Discover Service ", serviceName, " Error ", err)
			time.Sleep(2 * time.Second)
			continue
		}

		// mutex.Lock()
		for _, ip := range ips {
			logger.DiscoveryLog.Traceln("Discover Service ", serviceName, ", ip ", ip)
			// found := false
			if ipv4 := ip.To4(); ipv4 != nil {
				// for _, b := range backends {
				// logger.ConsumerLog.Warnln("dispatchAddServer Rlock")
				mutex.RLock()
				if _, ok := backends[ipv4.String()]; ok {
					mutex.RUnlock()
					// logger.ConsumerLog.Warnln("dispatchAddServer RUnlock")
					continue
				}
				// for b, _ := range backends {
				// 	if b.address == ipv4.String() {
				// 		found = true
				// 		break
				// 	}
				// }
				// if found == true {
				// 	continue
				// }
				mutex.RUnlock()
				// logger.ConsumerLog.Warnln("dispatchAddServer RUnlock")
				backend := &backendNF{}
				backend.address = ipv4.String()
				backend.count = 0
				// logger.ConsumerLog.Warnln("dispatchAddServer Lock")
				mutex.Lock()
				// backends = append(backends, backend)
				// backends[backend] = 0
				backends[ipv4.String()] = backend
				nfNum++
				mutex.Unlock()
				logger.DiscoveryLog.Infoln("New Server found IPv4: ", ipv4.String(), "backends: ", len(backends), " nfNum: ", nfNum)
				logger.DiscoveryLog.Infoln("backends: ", backends)
				// logger.ConsumerLog.Warnln("dispatchAddServer UnLock")
				go backend.connectToServer()
			}
		}
		// mutex.Unlock()
		time.Sleep(2 * time.Second)
	}
}

func (b *backendNF) connectionOnState() {

	go func() {

		// continue checking for state change
		// until one of break states is found
		for {
			change := b.conn.WaitForStateChange(ctxt.Background(), b.conn.GetState())
			if change && b.conn.GetState() == connectivity.Idle {
				logger.DiscoveryLog.Warnln("connectionOnState Idle deleting: ", b.address)
				b.deleteBackendNF()
				return
			}

		}
	}()

}

func (b *backendNF) deleteBackendNF() {
	// logger.DiscoveryLog.Warnln("deleteBackendNF Lock")
	logger.DiscoveryLog.Warnln("deleting backend ", b.address, " count: ", b.count)
	mutex.Lock()
	// for i, b1 := range backends {
	// 	if b1 == b {
	// 		backends[i] = backends[len(backends)-1]
	// 		backends = backends[:len(backends)-1]
	// 		nfNum--
	// 		break
	// 	}
	// }
	// for _, b1 := range backends {
	// delete(backends, b)
	delete(backends, b.address)
	nfNum--
	mutex.Unlock()
	logger.DiscoveryLog.Warnln("remaining backends ", len(backends), " nfNum: ", nfNum, " backends: ", backends)
	// fmt.Println("deleteBackendNF Unlock")
	// fmt.Println("deleteBackendNF RLock")
	// mutex.RLock()
	// for ip, b := range backends {
	// 	logger.DiscoveryLog.Warnln("Available backend %v count %v", ip, b.count)
	// }
	// mutex.RUnlock()
	// fmt.Println("deleteBackendNF RUnlock")
}

func (b *backendNF) readFromServer() {
	for {
		response, err := b.stream.Recv()
		// start_time := time.Now()
		if err != nil {
			log.Printf("Error in Recv %v, Stop listening for this server %v ", err, b.address)
			b.deleteBackendNF()
			return
		} else {
			if response.Msgtype == gClient.MsgType_INIT_MSG {
				log.Printf("Init Response from Server %s server: %s", response.AmfId, response.VerboseMsg)
			} else if response.Msgtype == gClient.MsgType_REDIRECT_MSG {
				redirectCount++
				logger.DiscoveryLog.Infof("Redirection redirectCount: %v", redirectCount)
				var found bool
				// for _, b1 := range backends {
				// fmt.Println("readFromServer RLock")
				mutex.RLock()
				// for b1, _ := range backends {
				if b1, ok := backends[response.RedirectId]; ok {
					// if b1.address == response.RedirectId {
					if b1.state == false {
						log.Printf("backend state is not in READY state, so not forwarding redirected Msg")
					} else {
						t := gClient.SctplbMessage{}
						t.VerboseMsg = "Hello From gNB Message !"
						t.Msgtype = gClient.MsgType_GNB_MSG
						t.SctplbId = os.Getenv("HOSTNAME")
						t.Msg = response.Msg
						t.GnbId = response.GnbId
						b1.stream.Send(&t)
						//log.Printf("successfully forwarded msg to correct AMF")
						found = true
					}
					// break
					// }
				}
				mutex.RUnlock()
				// fmt.Println("readFromServer RUnlock")
				if !found {
					log.Printf("dropping redirected message as backend ip [%v] is not exist", response.RedirectId)
				}
			} else {
				var ran *context.Ran
				//fetch ran connection based on GnbId
				if response.GnbId == "" {
					log.Printf("Received null GnbId from backend NF")
				} else if response.GnbIpAddr != "" {
					// GnbId may present NGSetupreponse/failure receives from NF
					ran, _ = context.Sctplb_Self().RanFindByGnbIp(response.GnbIpAddr)
					if ran != nil && response.GnbId != "" {
						ran.SetRanId(response.GnbId)
						// log.Printf("Received GnbId: %v for GNbIpAddress: %v from NF", response.GnbId, response.GnbIpAddr)
					}
				} else if response.GnbId != "" {
					ran, _ = context.Sctplb_Self().RanFindByGnbId(response.GnbId)
				}
				if ran != nil {
					ran.Conn.Write(response.Msg)
					// log.Printf("T2: %v sctplb-to-RAN-out backend %v ", time.Now().Sub(start_time).Nanoseconds(), b.address)
				} else {
					log.Printf("Couldn't fetch sctp connection with GnbId: %v", response.GnbId)
				}
			}
		}
	}
}

func (b *backendNF) connectToServer() {
	target := fmt.Sprintf("%s:%d", b.address, SimappConfig.Configuration.SctpGrpcPort)

	logger.DiscoveryLog.Warnln("Connecting to target ", target)

	var err error
	b.conn, err = grpc.Dial(target, grpc.WithInsecure())

	if err != nil {
		fmt.Println("did not connect: ", err)
		b.deleteBackendNF()
		return
	}

	//b.conn = conn
	b.gc = gClient.NewNgapServiceClient(b.conn)

	stream, err := b.gc.HandleMessage(ctxt.Background())
	if err != nil {
		log.Println("openn stream error ", err)
		b.deleteBackendNF()
		return
	}

	b.stream = stream
	b.state = true
	for {
		//INIT message to new NF instance
		context.Sctplb_Self().RanPool.Range(func(key, value interface{}) bool {
			req := gClient.SctplbMessage{}
			req.VerboseMsg = "Hello From SCTP LB !"
			req.Msgtype = gClient.MsgType_INIT_MSG
			req.SctplbId = os.Getenv("HOSTNAME")
			candidate := value.(*context.Ran)
			if candidate.RanId != "" {
				req.GnbId = candidate.RanId
			} else {
				log.Printf("ran connection %v is exist without GnbId, so not sending this ran details to NF",
					candidate.GnbIp)
				//return true
			}
			if err := stream.Send(&req); err != nil {
				log.Println("can not send: ", err)
			}
			// log.Printf("Send Request message : %v", req)
			response, err := stream.Recv()
			if err != nil {
				// log.Println("Response from server: error ", err)
				b.state = false
			} else {
				log.Printf("Init Response from Server %s server: %s", response.AmfId, response.VerboseMsg)
				b.state = true
			}
			return true
		})
		break
	}
	if b.state == true {
		go b.connectionOnState()
		go b.readFromServer()
	}
}

// func getAmfId(message *ngapType.NGAPPDU) *ngapType.AMFUENGAPID {
// func getAmfId(message *ngapType.NGAPPDU) (*ngapType.AMFUENGAPID, *ngapType.FiveGSTMSI) {
func getAmfId(message *ngapType.NGAPPDU) int64 {
	var aMFUENGAPID *ngapType.AMFUENGAPID
	var fiveGSTMSI *ngapType.FiveGSTMSI

	if message == nil {
		logger.SctpLog.Error("NGAP Message is nil")
		return 0
	}
	switch message.Present {
	case ngapType.NGAPPDUPresentInitiatingMessage:
		initiatingMessage := message.InitiatingMessage
		if initiatingMessage == nil {
			logger.SctpLog.Errorln("initiatingMessage is nil")
			return 0
		}
		switch initiatingMessage.ProcedureCode.Value {
		case ngapType.ProcedureCodeNGSetup:
		case ngapType.ProcedureCodeInitialUEMessage:
			ngapMsg := initiatingMessage.Value.InitialUEMessage
			if ngapMsg == nil {
				logger.SctpLog.Error("UplinkNasTransport is nil")
				return 0
			}
			for i := 0; i < len(ngapMsg.ProtocolIEs.List); i++ {
				ie := ngapMsg.ProtocolIEs.List[i]

				// logger.SctpLog.Infof("initiatingMessage.Value.InitialUEMessage: ie.Id.Value %v", ie.Id.Value)
				switch ie.Id.Value {
				case ngapType.ProtocolIEIDNASPDU: // reject
					nASPDU := []byte(ie.Value.NASPDU.Value)

					// logger.SctpLog.Infof("ie.Value: %v", ie.Value.NASPDU)
					msg := new(nas.Message)
					msg.SecurityHeaderType = nas.GetSecurityHeaderType(nASPDU) & 0x0f
					switch msg.SecurityHeaderType {
					case nas.SecurityHeaderTypeIntegrityProtected:
						p := nASPDU[7:]
						if err := msg.PlainNasDecode(&p); err != nil {
							return 0
						}
					case nas.SecurityHeaderTypePlainNas:
						// logger.CommLog.Infof("Security header type: PlainNas Message")
						if err := msg.PlainNasDecode(&nASPDU); err != nil {
							return 0
						}
					default:
						logger.SctpLog.Infof("Security header type is not plain or integrity protected")
						return 0
					}
					// logger.SctpLog.Infof("msg: %v", msg)
					// logger.SctpLog.Infof("msg.GmmHeader: %v", msg.GmmHeader)

					if msg.GmmHeader.GetMessageType() == nas.MsgTypeServiceRequest {
						mobileIdentity5GSContents := msg.ServiceRequest.TMSI5GS.Octet
						if nasMessage.MobileIdentity5GSType5gSTmsi == nasConvert.GetTypeOfIdentity(mobileIdentity5GSContents[0]) {
							// guti = StmsiToGuti(mobileIdentity5GSContents)
							// tmsi := guti[12:]
							buf := msg.ServiceRequest.TMSI5GS.Octet
							// tmsi5G := hex.EncodeToString(buf[3:])
							// logger.SctpLog.Infof("tmsi5G derived from Service Request Message: %v", tmsi5G)
							// byteToInt, _ := strconv.Atoi(buf[3:])
							byteToInt := binary.BigEndian.Uint32(buf[3:])
							// logger.SctpLog.Infof("tmsi5G derived from Service Request Message (int): %v", byteToInt)
							return int64(byteToInt)
						}
						// ran.Log.Trace("Decode IE NasPdu")
					}
					//  else {
					// 	logger.SctpLog.Infof("msg.GmmHeader is not MsgTypeServiceRequest")
					// }
				case ngapType.ProtocolIEIDFiveGSTMSI: // optional, reject
					fiveGSTMSI = ie.Value.FiveGSTMSI
					logger.SctpLog.Infof("found ie.Value.FiveGSTMSI", ie.Value.FiveGSTMSI)
					tmsi := hex.EncodeToString(fiveGSTMSI.FiveGTMSI.Value)
					byteToInt, _ := strconv.Atoi(tmsi)
					return int64(byteToInt)
				}
			}
		case ngapType.ProcedureCodeUplinkNASTransport:
			ngapMsg := initiatingMessage.Value.UplinkNASTransport
			if ngapMsg == nil {
				logger.SctpLog.Error("UplinkNasTransport is nil")
				return 0
			}
			for i := 0; i < len(ngapMsg.ProtocolIEs.List); i++ {
				ie := ngapMsg.ProtocolIEs.List[i]
				switch ie.Id.Value {
				case ngapType.ProtocolIEIDAMFUENGAPID:
					aMFUENGAPID = ie.Value.AMFUENGAPID
					return aMFUENGAPID.Value
				}
			}

		case ngapType.ProcedureCodeHandoverCancel:
			ngapMsg := initiatingMessage.Value.HandoverCancel
			for i := 0; i < len(ngapMsg.ProtocolIEs.List); i++ {
				ie := ngapMsg.ProtocolIEs.List[i]
				switch ie.Id.Value {
				case ngapType.ProtocolIEIDAMFUENGAPID:
					aMFUENGAPID = ie.Value.AMFUENGAPID
					return aMFUENGAPID.Value
				}
			}

		case ngapType.ProcedureCodeUEContextReleaseRequest:
			ngapMsg := initiatingMessage.Value.UEContextReleaseRequest
			for i := 0; i < len(ngapMsg.ProtocolIEs.List); i++ {
				ie := ngapMsg.ProtocolIEs.List[i]
				switch ie.Id.Value {
				case ngapType.ProtocolIEIDAMFUENGAPID:
					aMFUENGAPID = ie.Value.AMFUENGAPID
					logger.SctpLog.Trace("Decode IE AmfUeNgapID")
					if aMFUENGAPID == nil {
						logger.SctpLog.Error("AMFUENGAPID is nil")
						return 0
					}
					return aMFUENGAPID.Value
				}
			}
		case ngapType.ProcedureCodeNASNonDeliveryIndication:
			ngapMsg := initiatingMessage.Value.NASNonDeliveryIndication
			for i := 0; i < len(ngapMsg.ProtocolIEs.List); i++ {
				ie := ngapMsg.ProtocolIEs.List[i]
				switch ie.Id.Value {
				case ngapType.ProtocolIEIDAMFUENGAPID:
					aMFUENGAPID = ie.Value.AMFUENGAPID
					return aMFUENGAPID.Value
				}
			}
		case ngapType.ProcedureCodeLocationReportingFailureIndication:
		case ngapType.ProcedureCodeErrorIndication:
		case ngapType.ProcedureCodeUERadioCapabilityInfoIndication:
			ngapMsg := initiatingMessage.Value.UERadioCapabilityInfoIndication
			for i := 0; i < len(ngapMsg.ProtocolIEs.List); i++ {
				ie := ngapMsg.ProtocolIEs.List[i]
				switch ie.Id.Value {

				case ngapType.ProtocolIEIDAMFUENGAPID:
					aMFUENGAPID = ie.Value.AMFUENGAPID
					return aMFUENGAPID.Value
				}
			}

		case ngapType.ProcedureCodeHandoverNotification:
			ngapMsg := initiatingMessage.Value.HandoverNotify
			for i := 0; i < len(ngapMsg.ProtocolIEs.List); i++ {
				ie := ngapMsg.ProtocolIEs.List[i]
				switch ie.Id.Value {

				case ngapType.ProtocolIEIDAMFUENGAPID:
					aMFUENGAPID = ie.Value.AMFUENGAPID
					return aMFUENGAPID.Value
				}
			}

		case ngapType.ProcedureCodeHandoverPreparation:
			ngapMsg := initiatingMessage.Value.HandoverRequired
			for i := 0; i < len(ngapMsg.ProtocolIEs.List); i++ {
				ie := ngapMsg.ProtocolIEs.List[i]
				switch ie.Id.Value {

				case ngapType.ProtocolIEIDAMFUENGAPID:
					aMFUENGAPID = ie.Value.AMFUENGAPID
					return aMFUENGAPID.Value
				}
			}

		case ngapType.ProcedureCodeRANConfigurationUpdate:
		case ngapType.ProcedureCodeRRCInactiveTransitionReport:
		case ngapType.ProcedureCodePDUSessionResourceNotify:
			ngapMsg := initiatingMessage.Value.PDUSessionResourceNotify
			for i := 0; i < len(ngapMsg.ProtocolIEs.List); i++ {
				ie := ngapMsg.ProtocolIEs.List[i]
				switch ie.Id.Value {

				case ngapType.ProtocolIEIDAMFUENGAPID:
					aMFUENGAPID = ie.Value.AMFUENGAPID
					return aMFUENGAPID.Value
				}
			}

		case ngapType.ProcedureCodePathSwitchRequest:
			ngapMsg := initiatingMessage.Value.PathSwitchRequest
			for i := 0; i < len(ngapMsg.ProtocolIEs.List); i++ {
				ie := ngapMsg.ProtocolIEs.List[i]
				switch ie.Id.Value {
				case ngapType.ProtocolIEIDSourceAMFUENGAPID:
					aMFUENGAPID = ie.Value.SourceAMFUENGAPID
					logger.SctpLog.Trace("Decode IE AmfUeNgapID")
					if aMFUENGAPID == nil {
						logger.SctpLog.Error("AMFUENGAPID is nil")
						return 0
					}
					return aMFUENGAPID.Value
				}
			}

		case ngapType.ProcedureCodeLocationReport:
		case ngapType.ProcedureCodeUplinkUEAssociatedNRPPaTransport:
		case ngapType.ProcedureCodeUplinkRANConfigurationTransfer:
		case ngapType.ProcedureCodePDUSessionResourceModifyIndication:
			ngapMsg := initiatingMessage.Value.PDUSessionResourceModifyIndication
			for i := 0; i < len(ngapMsg.ProtocolIEs.List); i++ {
				ie := ngapMsg.ProtocolIEs.List[i]
				switch ie.Id.Value {
				case ngapType.ProtocolIEIDAMFUENGAPID:
					aMFUENGAPID = ie.Value.AMFUENGAPID
					logger.SctpLog.Trace("Decode IE AmfUeNgapID")
					if aMFUENGAPID == nil {
						logger.SctpLog.Error("AMFUENGAPID is nil")
						return 0
					}
					return aMFUENGAPID.Value
				}
			}

		case ngapType.ProcedureCodeCellTrafficTrace:
		case ngapType.ProcedureCodeUplinkRANStatusTransfer:
		case ngapType.ProcedureCodeUplinkNonUEAssociatedNRPPaTransport:
		}

	case ngapType.NGAPPDUPresentSuccessfulOutcome:
		successfulOutcome := message.SuccessfulOutcome
		if successfulOutcome == nil {
			logger.SctpLog.Errorln("successfulOutcome is nil")
			return 0
		}

		switch successfulOutcome.ProcedureCode.Value {
		case ngapType.ProcedureCodeNGReset:
		case ngapType.ProcedureCodeUEContextRelease:
			ngapMsg := successfulOutcome.Value.UEContextReleaseComplete
			for i := 0; i < len(ngapMsg.ProtocolIEs.List); i++ {
				ie := ngapMsg.ProtocolIEs.List[i]
				switch ie.Id.Value {
				case ngapType.ProtocolIEIDAMFUENGAPID:
					aMFUENGAPID = ie.Value.AMFUENGAPID
					logger.SctpLog.Trace("Decode IE AmfUeNgapID")
					if aMFUENGAPID == nil {
						logger.SctpLog.Error("AMFUENGAPID is nil")
						return 0
					}
					return aMFUENGAPID.Value
				}
			}

		case ngapType.ProcedureCodePDUSessionResourceRelease:
			ngapMsg := successfulOutcome.Value.PDUSessionResourceReleaseResponse
			for i := 0; i < len(ngapMsg.ProtocolIEs.List); i++ {
				ie := ngapMsg.ProtocolIEs.List[i]
				switch ie.Id.Value {

				case ngapType.ProtocolIEIDAMFUENGAPID:
					aMFUENGAPID = ie.Value.AMFUENGAPID
					return aMFUENGAPID.Value
				}
			}

		case ngapType.ProcedureCodeUERadioCapabilityCheck:
		case ngapType.ProcedureCodeAMFConfigurationUpdate:
		case ngapType.ProcedureCodeInitialContextSetup:
			ngapMsg := successfulOutcome.Value.InitialContextSetupResponse
			for i := 0; i < len(ngapMsg.ProtocolIEs.List); i++ {
				ie := ngapMsg.ProtocolIEs.List[i]
				switch ie.Id.Value {

				case ngapType.ProtocolIEIDAMFUENGAPID:
					aMFUENGAPID = ie.Value.AMFUENGAPID
					return aMFUENGAPID.Value
				}
			}

		case ngapType.ProcedureCodeUEContextModification:
			ngapMsg := successfulOutcome.Value.UEContextModificationResponse
			for i := 0; i < len(ngapMsg.ProtocolIEs.List); i++ {
				ie := ngapMsg.ProtocolIEs.List[i]
				switch ie.Id.Value {

				case ngapType.ProtocolIEIDAMFUENGAPID:
					aMFUENGAPID = ie.Value.AMFUENGAPID
					return aMFUENGAPID.Value
				}
			}

		case ngapType.ProcedureCodePDUSessionResourceSetup:
			ngapMsg := successfulOutcome.Value.PDUSessionResourceSetupResponse
			for i := 0; i < len(ngapMsg.ProtocolIEs.List); i++ {
				ie := ngapMsg.ProtocolIEs.List[i]
				switch ie.Id.Value {

				case ngapType.ProtocolIEIDAMFUENGAPID:
					aMFUENGAPID = ie.Value.AMFUENGAPID
					return aMFUENGAPID.Value
				}
			}

		case ngapType.ProcedureCodePDUSessionResourceModify:
			ngapMsg := successfulOutcome.Value.PDUSessionResourceModifyResponse
			for i := 0; i < len(ngapMsg.ProtocolIEs.List); i++ {
				ie := ngapMsg.ProtocolIEs.List[i]
				switch ie.Id.Value {

				case ngapType.ProtocolIEIDAMFUENGAPID:
					aMFUENGAPID = ie.Value.AMFUENGAPID
					return aMFUENGAPID.Value
				}
			}

		case ngapType.ProcedureCodeHandoverResourceAllocation:
			ngapMsg := successfulOutcome.Value.HandoverRequestAcknowledge
			for i := 0; i < len(ngapMsg.ProtocolIEs.List); i++ {
				ie := ngapMsg.ProtocolIEs.List[i]
				switch ie.Id.Value {
				case ngapType.ProtocolIEIDAMFUENGAPID:
					aMFUENGAPID = ie.Value.AMFUENGAPID
					logger.SctpLog.Trace("Decode IE AmfUeNgapID")
					if aMFUENGAPID == nil {
						logger.SctpLog.Error("AMFUENGAPID is nil")
						return 0
					}
					return aMFUENGAPID.Value
				}
			}
		}
	case ngapType.NGAPPDUPresentUnsuccessfulOutcome:
		unsuccessfulOutcome := message.UnsuccessfulOutcome
		if unsuccessfulOutcome == nil {
			logger.SctpLog.Errorln("unsuccessfulOutcome is nil")
			return 0
		}
		switch unsuccessfulOutcome.ProcedureCode.Value {
		case ngapType.ProcedureCodeAMFConfigurationUpdate:
		case ngapType.ProcedureCodeInitialContextSetup:
			ngapMsg := unsuccessfulOutcome.Value.InitialContextSetupFailure
			for i := 0; i < len(ngapMsg.ProtocolIEs.List); i++ {
				ie := ngapMsg.ProtocolIEs.List[i]
				switch ie.Id.Value {

				case ngapType.ProtocolIEIDAMFUENGAPID:
					aMFUENGAPID = ie.Value.AMFUENGAPID
					return aMFUENGAPID.Value
				}
			}

		case ngapType.ProcedureCodeUEContextModification:
			ngapMsg := unsuccessfulOutcome.Value.UEContextModificationFailure
			for i := 0; i < len(ngapMsg.ProtocolIEs.List); i++ {
				ie := ngapMsg.ProtocolIEs.List[i]
				switch ie.Id.Value {

				case ngapType.ProtocolIEIDAMFUENGAPID:
					aMFUENGAPID = ie.Value.AMFUENGAPID
					return aMFUENGAPID.Value
				}
			}

		case ngapType.ProcedureCodeHandoverResourceAllocation:
			ngapMsg := unsuccessfulOutcome.Value.HandoverFailure
			for i := 0; i < len(ngapMsg.ProtocolIEs.List); i++ {
				ie := ngapMsg.ProtocolIEs.List[i]
				switch ie.Id.Value {
				case ngapType.ProtocolIEIDAMFUENGAPID:
					aMFUENGAPID = ie.Value.AMFUENGAPID
					logger.SctpLog.Trace("Decode IE AmfUeNgapID")
					if aMFUENGAPID == nil {
						logger.SctpLog.Error("AMFUENGAPID is nil")
						return 0
					}
					return aMFUENGAPID.Value
				}
			}
		}
	}
	return 0
}

// func dispatchMessage(conn net.Conn, msg []byte, start_time time.Time) { //*gClient.Message) {
func dispatchMessage(conn net.Conn, msg []byte) { //*gClient.Message) {
	// start_time := time.Now()
	// log.Printf("%v sctplb-in", start_time)
	// add this message for one of the client
	// select server who can handle this message.. round robin
	// add message in the server queue
	// select the server which is connected

	// Implement rate limit per gNb here
	// implement per site rate limit here
	var peer *SctpConnections
	p, ok := connections.Load(conn)
	if !ok {
		logger.SctpLog.Infof("Notification for unknown connection")
		return
	} else {
		peer = p.(*SctpConnections)
		//logger.SctpLog.Warnf("Handle SCTP Notification[addr: %+v], peer %v ", conn.RemoteAddr(), peer)
	}

	ran, _ := context.Sctplb_Self().RanFindByConn(conn)
	if len(msg) == 0 {
		logger.SctpLog.Infof("send Gnb connection [%v] close message to all AMF Instances", peer)
		t := gClient.SctplbMessage{}
		t.VerboseMsg = "Bye From gNB Message !"
		t.Msgtype = gClient.MsgType_GNB_DISC
		t.SctplbId = os.Getenv("HOSTNAME")
		if ran != nil && ran.RanId != "" {
			t.GnbId = ran.RanId
		}
		t.Msg = msg
		// if backends != nil && len(backends) > 0 {
		if len(backends) > 0 {
			// var i int
			// for ; i < len(backends); i++ {
			// 	backend := backends[i]
			// fmt.Println("dispatchMessage RLock")
			mutex.RLock()
			for _, backend := range backends {
				if backend.state == true {
					if err := backend.stream.Send(&t); err != nil {
						logger.SctpLog.Errorln("can not send ", err)
					}
				}
			}
			mutex.RUnlock()
			// fmt.Println("dispatchMessage RUnlock")
		} else {
			logger.SctpLog.Errorln("No AMF Connections")
		}
		ran.Log.Infof("Delete RAN Context[ID: %+v]", ran.RanID())
		context.Sctplb_Self().DeleteRan(conn)
		return
	}
	if ran == nil {
		ran = context.Sctplb_Self().NewRan(conn)
	}
	pdu, err := ngap.Decoder(msg)
	if err != nil {
		ran.Log.Errorf("NGAP decode error : %+v", err)
		return
	}

	var amfId int64 = 0
	// aMFUENGAPID, fiveGSTMSI := getAmfId(pdu)
	amfId = getAmfId(pdu)

	// logger.SctpLog.Println("Message received from remoteAddr ", conn.RemoteAddr().String())
	t := gClient.SctplbMessage{}
	t.VerboseMsg = "Hello From gNB Message !"
	t.Msgtype = gClient.MsgType_GNB_MSG
	t.SctplbId = os.Getenv("HOSTNAME")
	//send GnbId to backendNF if exist
	//GnbIp to backend ig GnbId is not exist, mostly this is for NGSetup Message
	if ran.RanId != "" {
		t.GnbId = ran.RanId
	} else {
		t.GnbIpAddr = conn.RemoteAddr().String()
	}
	t.Msg = msg
	if len(backends) == 0 {
		fmt.Println("NO backend available")
		return
	}
	var i int
	for ; i < len(backends); i++ {
		// Select the backend NF based on RoundRobin Algorithm
		// a. For initial message load balancing is Round Robin & for uplink transport messages
		//    load balancing is based on hashing.
		// b. Redirect support in AMF<-->SCTPLB is still required because under some corner cases
		//    it is possible that message may go to wrong AMF
		// c. If number of AMF instances are more then which means more redirect messages which means 2 times SCTP message decode
		//    so it makes sense to add NGAP decoding support at SCTPLB itself.
		//
		// TBD : 1) Use DRSM to send request to correct AMF or
		//          Use consistent hashing to send message to AMF which owns the hash index
		//       2) NAS decoding to fetch TMSI and send message to one of the AMF using hashing or DRSM
		backend := MinLogic(amfId)
		if backend != nil {
			if err := backend.stream.Send(&t); err != nil {
				logger.SctpLog.Errorln("can not send: ", err)
			}

			// log.Printf("T1: %v sctplb-to-AMF time difference backend %v ", time.Now().Sub(start_time).Nanoseconds(), backend.address)
			break
		}
	}
}
