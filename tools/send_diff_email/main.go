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
	"io/ioutil"
	"log"
	"net/smtp"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	secretmanagerpb "google.golang.org/genproto/googleapis/cloud/secretmanager/v1"
)

var (
	basebt = flag.String("base_bt", "", "Base Bigtable version.")
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
		"datacommons+release @google.com",
	}

	// smtp server configuration.
	smtpHost := "smtp.gmail.com"
	smtpPort := "587"

	diff, err := ioutil.ReadFile("/tmp/golden-diff.html")
	if err != nil {
		log.Fatal(err)
	}

	subject := fmt.Sprintf("Subject: Mixer golden data diff from %s\n", *basebt)
	mime := "MIME-version: 1.0;\nContent-Type: text/html;\n\n"
	content := string(diff)
	body := subject + mime + content

	// Authentication.
	auth := smtp.PlainAuth("", from, password, smtpHost)

	// Sending email.
	err = smtp.SendMail(smtpHost+":"+smtpPort, auth, from, to, []byte(body))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Email Sent Successfully!")
}
