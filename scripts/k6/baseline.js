import http from 'k6/http';
import { check, fail } from 'k6';

const baseURL = __ENV.BASE_URL || 'http://gateway:8080';
const vus = Number(__ENV.VUS || 100);
const duration = __ENV.DURATION || '5s';

export const options = {
  vus,
  duration,
  summaryTrendStats: ['min', 'med', 'avg', 'p(90)', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    http_req_failed: ['rate<0.01'],
    checks: ['rate>0.99'],
  },
};

export function setup() {
  const productResponse = http.post(
    `${baseURL}/api/v1/admin/product`,
    JSON.stringify({ name: `m1-baseline-${Date.now()}`, price: '99.99', stock: 100000 }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  if (!check(productResponse, { 'product created': (r) => r.status === 201 })) {
    fail(`product setup failed: ${productResponse.body}`);
  }
  const productID = productResponse.json('product.id');
  const activityResponse = http.post(
    `${baseURL}/api/v1/admin/activity`,
    JSON.stringify({
      product_id: productID,
      start_time: new Date(Date.now() - 60_000).toISOString(),
      end_time: new Date(Date.now() + 3_600_000).toISOString(),
      seckill_price: '9.99',
      stock: 100000,
    }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  if (!check(activityResponse, { 'activity created': (r) => r.status === 201 })) {
    fail(`activity setup failed: ${activityResponse.body}`);
  }
  return { productID, activityID: activityResponse.json('activity.id') };
}

export default function (data) {
  const userID = __VU * 10_000_000 + __ITER + 1;
  const response = http.post(
    `${baseURL}/api/v1/seckill`,
    JSON.stringify({ activity_id: data.activityID, product_id: data.productID, user_id: userID }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  check(response, { 'purchase accepted': (r) => r.status === 201 && r.json('code') === 0 });
}
