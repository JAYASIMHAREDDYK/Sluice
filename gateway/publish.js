export async function publish(producer, topic, message) {
  const key = `${message.tenant_id}:${message.account_id}`;
  await producer.send({
    topic,
    acks: -1,
    messages: [
      {
        key,
        value: JSON.stringify(message)
      }
    ]
  });
}
