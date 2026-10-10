import http from 'k6/http';
import { check } from 'k6';

export const options = {
  stages: [
    { duration: '10s', target: 50 },
    { duration: '20s', target: 100 },
    { duration: '10s', target: 0 }
  ],
  thresholds: {
    http_req_failed: ['rate<0.01']
  }
};

export default function () {
  const tenantId = `tenant_${__VU % 10}`;
  const accountId = `acc_${__VU}`;
  const idempotencyKey = `k6_${__VU}_${__ITER}`;

  const payload = JSON.stringify({
    tenant_id: tenantId,
    account_id: accountId,
    amount: 10,
    idempotency_key: idempotencyKey
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'x-tenant-id': tenantId,
      'x-idempotency-key': idempotencyKey
    }
  };

  const url = __ENV.TARGET_URL || 'http://host.docker.internal:3000/transactions';
  const res = http.post(url, payload, params);

  check(res, {
    'status is 202': (r) => r.status === 202
  });
}
