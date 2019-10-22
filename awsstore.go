package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"strings"
)

func main() {
	lambda.Start(handler)
}

type MessageFiltered struct {
	BucketName  string `json:"bucketname"`
	KeyName     string `json:"keyname"`
	EventName   string `json:"eventname"`
	EventSource string `json:"eventsource"`
}

//-----------------------------------------------
//Handler to pick Aws S3 Events
//-----------------------------------------------
func handler(ctx context.Context, events events.S3Event) {

	sess := session.Must(session.NewSessionWithOptions(session.Options{SharedConfigState: session.SharedConfigEnable, Config: aws.Config{Region: aws.String("ap-southeast-2")}}))
	svc := sqs.New(sess)

	queueURL, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String("optima-parserstore-test"),
	})
	if err != nil {
		fmt.Println("error in getting queue url")
	}
	for _, record := range events.Records {
		s3 := record.S3
		if strings.Contains(strings.ToLower(record.EventName), "put") ||
			strings.Contains(strings.ToLower(record.EventName), "multipart") ||
			strings.Contains(strings.ToLower(record.EventName), "copy") ||
			strings.Contains(strings.ToLower(record.EventName), "post") {

			message := MessageFiltered{
				BucketName:  s3.Bucket.Name,
				KeyName:     s3.Object.Key,
				EventName:   record.EventName,
				EventSource: record.EventSource,
			}
			jsonData, err := json.Marshal(message)
			if err != nil {
				fmt.Println("error in json serializing")
			}
			_, errs := svc.SendMessage(&sqs.SendMessageInput{
				QueueUrl:     queueURL.QueueUrl,
				MessageBody:  aws.String(string(jsonData)),
				DelaySeconds: aws.Int64(0),
			})
			if errs != nil {
				fmt.Println("error on sending message", errs)
			}

		}

	}
}
