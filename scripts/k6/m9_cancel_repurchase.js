import http from 'k6/http';
import { check, fail, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const baseURL = __ENV.BASE_URL || 'http://gateway:8080';
const releasedAccepted = new Counter('released_stock_accepted');
// 刚被接受的异步订单在 Kafka 事件仍 Pending 时可能合理地返回 404；
// setup 会显式轮询，直到状态发生变化。
http.setResponseCallback(http.expectedStatuses(200, 201, 202, 404, 409));

export const options = {
  scenarios: {
    reclaim: { executor: 'per-vu-iterations', vus: Number(__ENV.VUS || 100), iterations: 1, maxDuration: '30s' },
  },
  summaryTrendStats: ['min', 'med', 'avg', 'p(90)', 'p(95)', 'p(99)', 'max'],
  thresholds: { http_req_failed: ['rate==0'], checks: ['rate>0.99'] },
};

export function setup() {
  const stock = 5;
  const product = http.post(`${baseURL}/api/v1/admin/product`, JSON.stringify({ name: `m9-cancel-${Date.now()}`, price: '9.99', stock }), { headers: { 'Content-Type': 'application/json' } });
  if (!check(product, { 'product created': (r) => r.status === 201 })) fail(product.body);
  const productID = product.json('product.id');
  const activity = http.post(`${baseURL}/api/v1/admin/activity`, JSON.stringify({ product_id: productID, start_time: new Date(Date.now() - 60_000).toISOString(), end_time: new Date(Date.now() + 3_600_000).toISOString(), seckill_price: '4.99', stock }), { headers: { 'Content-Type': 'application/json' } });
  if (!check(activity, { 'activity created': (r) => r.status === 201 })) fail(activity.body);
  const activityID = activity.json('activity.id');
  const warmup = http.post(`${baseURL}/api/v1/admin/activity/${activityID}/warmup`);
  if (!check(warmup, { 'stock warmed': (r) => r.status === 200 })) fail(warmup.body);

  const orders = [];
  for (let userID = 1; userID <= stock; userID += 1) {
    const buy = http.post(`${baseURL}/api/v1/seckill`, JSON.stringify({ activity_id: activityID, product_id: productID, user_id: userID }), { headers: { 'Content-Type': 'application/json' } });
    if (!check(buy, { 'initial reservation accepted': (r) => r.status === 202 })) fail(buy.body);
    orders.push({ orderID: buy.json('order_id_string'), userID });
  }
  for (const order of orders) {
    let created = false;
    for (let attempt = 0; attempt < 100; attempt += 1) {
      const read = http.get(`${baseURL}/api/v1/orders/${order.orderID}`);
      if (read.status === 200 && read.json('order.status') === 'created') { created = true; break; }
      sleep(0.05);
    }
    if (!created) fail(`order ${order.orderID} was not created before cancellation`);
    const cancel = http.post(`${baseURL}/api/v1/orders/${order.orderID}/cancel`, null, { headers: { 'X-Development-User-ID': String(order.userID) } });
    if (!check(cancel, { 'cancel accepted': (r) => r.status === 202 || r.status === 200 })) fail(cancel.body);
    let cancelled = false;
    for (let attempt = 0; attempt < 100; attempt += 1) {
      const read = http.get(`${baseURL}/api/v1/orders/${order.orderID}`);
      if (read.status === 200 && read.json('order.status') === 'cancelled') { cancelled = true; break; }
      sleep(0.05);
    }
    if (!cancelled) fail(`order ${order.orderID} was not released`);
  }
  return { productID, activityID };
}

export default function (data) {
  const userID = 100_000 + __VU;
  const response = http.post(`${baseURL}/api/v1/seckill`, JSON.stringify({ activity_id: data.activityID, product_id: data.productID, user_id: userID }), { headers: { 'Content-Type': 'application/json' } });
  if (response.status === 202) releasedAccepted.add(1);
  check(response, { 'reclaim either gets released stock or clean exhaustion': (r) => r.status === 202 || r.status === 409 });
}
