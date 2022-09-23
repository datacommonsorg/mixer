// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/smtp"
	"os"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	secretmanagerpb "google.golang.org/genproto/googleapis/cloud/secretmanager/v1"
)

var (
	subject  = flag.String("subject", "", "Email subject.")
	receiver = flag.String("receiver", "", "Email receiver.")
	bodyFile = flag.String("body_file", "", "Email body content file.")
	mimeType = flag.String("mime_type", "text/html", "Mime type of the body.")
)

func main() {
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Get the password.
	ctx := context.Background()
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		log.Fatalf("failed to setup client: %v", err)
	}
	accessRequest := &secretmanagerpb.AccessSecretVersionRequest{
		Name: "projects/datcom-ci/secrets/gmailpw/versions/3",
	}
	result, err := client.AccessSecretVersion(ctx, accessRequest)
	if err != nil {
		log.Fatalf("failed to access secret version: %v", err)
	}

	// Sender data.
	from := "datacommonsorg@gmail.com"
	password := string(result.Payload.Data)

	// Receiver email address.
	to := []string{
		*receiver,
	}

	// smtp server configuration.
	smtpHost := "smtp.gmail.com"
	smtpPort := "587"

	bodyByte, err := os.ReadFile(*bodyFile)
	if err != nil {
		log.Fatal(err)
	}

	subject := fmt.Sprintf("Subject: %s\n", *subject)
	mime := fmt.Sprintf("MIME-version: 1.0;\nContent-Type: %s;\n\n", *mimeType)
	body := string(bodyByte)
	msg := subject + mime + body

	// Authentication.
	auth := smtp.PlainAuth("", from, password, smtpHost)

	// Sending email.
	err = smtp.SendMail(smtpHost+":"+smtpPort, auth, from, to, []byte(msg))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Email Sent Successfully!")
}
