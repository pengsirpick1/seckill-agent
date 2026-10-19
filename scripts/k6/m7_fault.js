import http from 'k6/http';
import { Counter } from 'k6/metrics';
import { check, fail } from 'k6';

const baseURL = __ENV.BASE_URL || 'http://gateway:8080';
const rate = Number(__ENV.RATE || 80);
const duration = __ENV.DURATION || '15s';
const allowLockTimeout = __ENV.ALLOW_503 === 'true';
const accepted = new Counter('accepted_202');
const rateLimited = new Counter('rate_limited_429');

http.setResponseCallback(http.expectedStatuses(201, 200, 202, 429, 503));

export const options = {
  scenarios: {
    controlled_load: {
      executor: 'constant-arrival-rate',
      rate,
      timeUnit: '1s',
      duration,
      preAllocatedVUs: 50,
      maxVUs: 200,
      gracefulStop: '5s',
    },
  },
  summaryTrendStats: ['min', 'med', 'avg', 'p(90)', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    http_req_failed: ['rate<0.01'],
    checks: ['rate>0.99'],
  },
};

export function setup() {
  const headers = { headers: { 'Content-Type': 'application/json' } };
  const product = http.post(baseURL + '/api/v1/admin/product', JSON.stringify({ name: 'm7-fault-' + Date.now(), price: '99.99', stock: 100000 }), headers);
  if (!check(product, { 'product created': (r) => r.status === 201 })) fail(product.body);
  const productID = product.json('product.id');
  const activity = http.post(baseURL + '/api/v1/admin/activity', JSON.stringify({
    product_id: productID,
    start_time: new Date(Date.now() - 60_000).toISOString(),
    end_time: new Date(Date.now() + 3_600_000).toISOString(),
    seckill_price: '9.99',
    stock: 100000,
  }), headers);
  if (!check(activity, { 'activity created': (r) => r.status === 201 })) fail(activity.body);
  const activityID = activity.json('activity.id');
  const warmup = http.post(baseURL + '/api/v1/admin/activity/' + activityID + '/warmup');
  if (!check(warmup, { 'stock warmed': (r) => r.status === 200 })) fail(warmup.body);
  return { productID, activityID };
}

export default function (data) {
  const userID = __VU * 10_000_000 + __ITER + 1;
  const response = http.post(baseURL + '/api/v1/seckill', JSON.stringify({
    activity_id: data.activityID,
    product_id: data.productID,
    user_id: userID,
  }), { headers: { 'Content-Type': 'application/json' } });
  if (response.status === 202) accepted.add(1);
  if (response.status === 429) rateLimited.add(1);
  check(response, {
    'request accepted or expected rejection': (r) => (r.status === 202 && r.json('code') === 0) || r.status === 429 || (allowLockTimeout && r.status === 503),
  });
}
