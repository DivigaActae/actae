// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"net/url"
	"os"

	actae "github.com/actae-dev/actae/sdks/go"
)

func main() {
	c, err := actae.NewFleetClient(actae.FleetClientOptions{Endpoint: os.Getenv("ACTAE_FLEET_URL"), OrganizationToken: os.Getenv("ACTAE_ORG_TOKEN"), OrganizationID: os.Getenv("ACTAE_ORG_ID")})
	if err != nil { panic(err) }
	ctx := context.Background()
	instances, err := c.Instances(ctx); if err != nil { panic(err) }
	fmt.Println("instances:", instances)
	query := url.Values{"channel_ids": {os.Getenv("ACTAE_CHANNEL_ID")}, "limit": {"20"}}
	result, err := c.Query(ctx, os.Getenv("ACTAE_INSTANCE_ID"), query); if err != nil { panic(err) }
	fmt.Println("events:", result)
}
