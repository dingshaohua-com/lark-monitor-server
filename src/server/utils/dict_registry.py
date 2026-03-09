_REGISTRY: dict[str, dict] = {}


def register(name: str, data: dict) -> None:
    _REGISTRY[name] = data


def get(name: str) -> dict | None:
    return _REGISTRY.get(name)


def get_all() -> dict[str, dict]:
    return _REGISTRY


def list_names() -> list[str]:
    return list(_REGISTRY.keys())


# ── 工单字段：中文 → 英文 ──

register("field_name_map", {
    "用户原文": "user_content",
    "功能异常": "func_exception",
    "优先级": "priority",
    "客户端": "client_type",
    "模块": "module",
    "内容标签": "content_tag",
    "反馈ID": "feedback_id",
    "分类": "category",
    "一级标签": "tag_l1",
    "二级标签": "tag_l2",
    "三级标签": "tag_l3",
    "学段ID": "education_stage_id",
    "学科ID": "subject_id",
    "课程ID": "course_id",
    "课程版本": "course_version",
    "题集ID": "question_set_id",
    "题集版本": "question_set_version",
    "题目ID": "question_id",
    "题目版本": "question_version",
    "知识点ID": "knowledge_id",
    "词书ID": "wordbook_id",
    "单词组ID": "word_group_id",
    "单词ID": "word_id",
    "组件ID": "component_id",
    "组件版本": "component_version",
    "组件索引": "component_index",
    "版本号": "app_version",
    "设备型号": "device_model",
    "来源学校ID": "school_id",
    "来源学校名称": "school_name",
    "年级名称": "grade_name",
    "班级名称": "class_name",
    "学科名称": "subject_name",
    "姓名": "student_name",
    "uid": "uid",
    "线上反馈时间": "feedback_time",
    "所属客服": "customer_service",
    "客服备注": "cs_remark",
    "文档ID": "doc_id",
    "播放时间线": "play_timeline",
    "扩展信息": "extra_info",
    "载荷链接": "payload_url",
    "查看线上版本": "online_version_url",
})
