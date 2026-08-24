# filename: validators.py
# description: быстрый маркерный анализатор (Уровень 3)

import re

CONSTRUCTION_MARKERS = [
    r"бетон", r"щебень", r"песок", r"арматур", r"цемент", r"раствор", r"куб", r"тонн",
    r"доставк", r"привез", r"заказ", r"машин", r"самосвал", r"камаз", r"объект",
    r"выгруз", r"поставк", r"плит", r"блок", r"геодез", r"высот", r"отметк", r"ось", r"секци"
]

CHITCHAT_MARKERS = [
    r"^привет", r"^здравствуй", r"как дела", r"спасибо", r"благодарю", r"от души",
    r"хай", r"ку", r"добрый (день|вечер|утро)", r"ты кто", r"расскажи о себе"
]


def fast_surface_validate(text: str) -> dict:
    text_lower = text.lower().strip()

    construction_hits = sum(1 for m in CONSTRUCTION_MARKERS if re.search(m, text_lower))
    chitchat_hits = sum(1 for m in CHITCHAT_MARKERS if re.search(m, text_lower))

    if construction_hits > 0:
        return {
            "intent_type": "construction_task",
            "confidence_score": min(50 + (construction_hits * 15), 95),
            "priority": 3 if construction_hits > 1 else 2,
            "is_valid": True
        }

    if chitchat_hits > 0:
        return {
            "intent_type": "chitchat",
            "confidence_score": 85,
            "priority": 1,
            "is_valid": True
        }

    return {
        "intent_type": "unknown",
        "confidence_score": 20,
        "priority": 1,
        "is_valid": False
    }
