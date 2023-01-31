package grpcapi

import (
	"context"
	"database/sql"
	"log"
	"net"
	"time"

	"github.com/raphaelmb/go-mailinglist-ms/mdb"
	"github.com/raphaelmb/go-mailinglist-ms/proto"
	"google.golang.org/grpc"
)

type MailServer struct {
	proto.UnimplementedMailingListServiceServer
	db *sql.DB
}

func pbEntryToMdbEntry(pbEntry *proto.EmailEntry) mdb.EmailEntry {
	t := time.Unix(pbEntry.ConfirmedAt, 0)
	return mdb.EmailEntry{
		Id:          pbEntry.Id,
		Email:       pbEntry.Email,
		ConfirmedAt: &t,
		OptOut:      pbEntry.OptOut,
	}
}

func mdbEntryToPbEntry(mdbEntry *mdb.EmailEntry) proto.EmailEntry {
	return proto.EmailEntry{
		Id:          mdbEntry.Id,
		Email:       mdbEntry.Email,
		ConfirmedAt: mdbEntry.ConfirmedAt.Unix(),
		OptOut:      mdbEntry.OptOut,
	}
}

func emailResponse(db *sql.DB, email string) (*proto.EmailResponse, error) {
	entry, err := mdb.GetEmail(db, email)
	if err != nil {
		return &proto.EmailResponse{}, err
	}
	if entry == nil {
		return &proto.EmailResponse{}, nil
	}

	res := mdbEntryToPbEntry(entry)

	return &proto.EmailResponse{EmailEntry: &res}, nil
}

func (ms *MailServer) GetEmail(ctx context.Context, req *proto.GetEmailRequest) (*proto.EmailResponse, error) {
	log.Printf("gRPC GetEmail: %v\n", req)
	return emailResponse(ms.db, req.EmailAddr)
}

func (ms *MailServer) GetEmailBatch(ctx context.Context, req *proto.GetEmailBatchRequest) (*proto.GetEmailBatchResponse, error) {
	log.Printf("gRPC GetEmailBatch: %v\n", req)
	params := mdb.GetEmailBatchQueryParams{
		Page:  int(req.Page),
		Count: int(req.Count),
	}

	mdbEntries, err := mdb.GetEmailBatch(ms.db, params)
	if err != nil {
		return &proto.GetEmailBatchResponse{}, err
	}

	pbEntires := make([]*proto.EmailEntry, 0, len(mdbEntries))

	for i := 0; i < len(mdbEntries); i++ {
		entry := mdbEntryToPbEntry(&mdbEntries[i])
		pbEntires = append(pbEntires, &entry)
	}

	return &proto.GetEmailBatchResponse{EmailEntries: pbEntires}, nil
}

func (ms *MailServer) CreateEmail(ctx context.Context, req *proto.CreateEmailRequest) (*proto.EmailResponse, error) {
	log.Printf("gRPC CreateEmail: %v\n", req)
	err := mdb.CreateEmail(ms.db, req.EmailAddr)
	if err != nil {
		return &proto.EmailResponse{}, err
	}
	return emailResponse(ms.db, req.EmailAddr)
}

func (ms *MailServer) UpdateEmail(ctx context.Context, req *proto.UpdateEmailRequest) (*proto.EmailResponse, error) {
	log.Printf("gRPC UpdateEmail: %v\n", req)
	entry := pbEntryToMdbEntry(req.EmailEntry)

	err := mdb.UpdateEmail(ms.db, entry)
	if err != nil {
		return &proto.EmailResponse{}, err
	}
	return emailResponse(ms.db, entry.Email)
}

func (ms *MailServer) DeleteEmail(ctx context.Context, req *proto.DeleteEmailRequest) (*proto.EmailResponse, error) {
	log.Printf("gRPC DeleteEmail: %v\n", req)

	err := mdb.DeleteEmail(ms.db, req.EmailAddr)
	if err != nil {
		return &proto.EmailResponse{}, err
	}
	return emailResponse(ms.db, req.EmailAddr)
}

func Serve(db *sql.DB, bind string) {
	listener, err := net.Listen("tcp", bind)
	if err != nil {
		log.Fatalf("gRPC server error: failure to bind %v\n", bind)
	}

	grpcServer := grpc.NewServer()
	mailServer := MailServer{db: db}
	proto.RegisterMailingListServiceServer(grpcServer, &mailServer)
	log.Printf("gRPC API server listening on %v\n", bind)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("gRPC server error: %v\n", err)
	}
}
