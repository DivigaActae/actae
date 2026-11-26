// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { FleetClient } from '@actae/sdk';

const fleet = new FleetClient({
  endpoint: process.env.ACTAE_FLEET_URL!,
  organizationToken: process.env.ACTAE_ORG_TOKEN!,
  organizationId: process.env.ACTAE_ORG_ID!,
});
const instance = process.env.ACTAE_INSTANCE_ID!;
console.log(await fleet.instances());
console.log(await fleet.query(instance, { channel_ids: [process.env.ACTAE_CHANNEL_ID ?? 'run'], limit: 20 }));

// For browser deployments use `fleetStream({url, ticket})`; never put the
// organization token in a browser bundle.
