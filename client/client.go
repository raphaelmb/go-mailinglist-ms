package main

import (
	"context"
	"log"
	"time"

	"github.com/alexflint/go-arg"
	"github.com/raphaelmb/go-mailinglist-ms/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func logResponse(r *proto.EmailResponse, err error) {
	if err != nil {
		log.Fatalf("  error: %v", err)
	}

	if r.EmailEntry == nil {
		log.Printf("  email not found")
	} else {
		log.Printf("  response: %v", r.EmailEntry)
	}

}

func createEmail(client proto.MailingListServiceClient, addr string) *proto.EmailEntry {
	log.Println("create email")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := client.CreateEmail(ctx, &proto.CreateEmailRequest{EmailAddr: addr})
	logResponse(res, err)

	return res.EmailEntry
}

func getEmail(client proto.MailingListServiceClient, addr string) *proto.EmailEntry {
	log.Println("get email")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := client.GetEmail(ctx, &proto.GetEmailRequest{EmailAddr: addr})
	logResponse(res, err)

	return res.EmailEntry
}

func getEmailBatch(client proto.MailingListServiceClient, count int, page int) {
	log.Println("get email batch")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := client.GetEmailBatch(ctx, &proto.GetEmailBatchRequest{Count: int32(count), Page: int32(page)})
	if err != nil {
		log.Fatalf("  error: %v", err)
	}
	log.Println("response:")
	for i, entry := range res.EmailEntries {
		log.Printf("  item [%v of %v]: %s", i+1, len(res.EmailEntries), entry)
	}
}

func updateEmail(client proto.MailingListServiceClient, entry proto.EmailEntry) *proto.EmailEntry {
	log.Println("update email")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := client.UpdateEmail(ctx, &proto.UpdateEmailRequest{EmailEntry: &entry})
	logResponse(res, err)

	return res.EmailEntry
}

func deleteEmail(client proto.MailingListServiceClient, addr string) *proto.EmailEntry {
	log.Println("delete email")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := client.DeleteEmail(ctx, &proto.DeleteEmailRequest{EmailAddr: addr})
	logResponse(res, err)

	return res.EmailEntry
}

var args struct {
	GrpcAddr string `arg:"env:MAILINGLIST_GRPC_ADDR"`
}

func main() {
	arg.MustParse(&args)
	if args.GrpcAddr == "" {
		args.GrpcAddr = ":8081"
	}

	conn, err := grpc.Dial(args.GrpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := proto.NewMailingListServiceClient(conn)

	newEmail := createEmail(client, "client@c.com")
	newEmail.ConfirmedAt = 10000
	updateEmail(client, *newEmail)
	deleteEmail(client, newEmail.Email)
	getEmailBatch(client, 5, 1)
}
