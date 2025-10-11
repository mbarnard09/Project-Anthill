import aiohttp
import asyncio
import json
from typing import List, Dict, Optional
import requests


DEEPSEEK_API_URL = "https://api.deepseek.com/chat/completions"
DEEPSEEK_MODEL = "deepseek-chat"


def deepseek_chat(messages: List[Dict[str, str]], api_key: str, temperature: float = 0.0, max_tokens: int = 64) -> str:
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    payload = {
        "model": DEEPSEEK_MODEL,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "messages": messages,
    }
    resp = requests.post(DEEPSEEK_API_URL, headers=headers, data=json.dumps(payload), timeout=30)
    resp.raise_for_status()
    data = resp.json()
    try:
        return (data["choices"][0]["message"]["content"] or "").strip()
    except Exception:
        return ""


async def deepseek_chat_async(messages: List[Dict[str, str]], api_key: str, temperature: float = 0.0, max_tokens: int = 64) -> str:
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    payload = {
        "model": DEEPSEEK_MODEL,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "messages": messages,
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(DEEPSEEK_API_URL, headers=headers, json=payload, timeout=30) as response:
            response.raise_for_status()
            data = await response.json()
            try:
                return (data["choices"][0]["message"]["content"] or "").strip()
            except Exception:
                return ""

