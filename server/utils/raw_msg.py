
def message_to_doc(msg) -> dict:
    """将飞书 SDK Message 对象转为 MongoDB 文档。"""
    doc = {
        "_id": msg.message_id,
        "message_id": msg.message_id,
        "msg_type": msg.msg_type,
        "create_time": msg.create_time,
        "update_time": msg.update_time,
        "chat_id": msg.chat_id,
        "root_id": msg.root_id,
        "parent_id": msg.parent_id,
        "thread_id": msg.thread_id,
        "upper_message_id": msg.upper_message_id,
        "deleted": msg.deleted,
        "updated": msg.updated,
    }

    if msg.body:
        doc["body"] = {"content": msg.body.content}

    if msg.sender:
        doc["sender"] = {
            "id": msg.sender.id,
            "id_type": msg.sender.id_type,
            "sender_type": msg.sender.sender_type,
            "tenant_key": msg.sender.tenant_key,
        }

    if msg.mentions:
        doc["mentions"] = [
            {
                "key": m.key,
                "id": m.id,
                "id_type": m.id_type,
                "name": m.name,
                "tenant_key": m.tenant_key,
            }
            for m in msg.mentions
        ]

    return doc
