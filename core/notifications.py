import os
import aiohttp
import asyncio

class NotificationManager:
    def __init__(self):
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"

    async def send_alert(self, message: str):
        """Sends a Telegram notification."""
        if not self.bot_token or not self.chat_id:
            print("⚠️ Telegram credentials missing. Skipping alert.")
            return

        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": "Markdown"
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.base_url, json=payload) as resp:
                    if resp.status != 200:
                        print(f"Failed to send TG alert: {await resp.text()}")
                    else:
                        print("✅ Telegram Alert Sent")
        except Exception as e:
            print(f"TG Alert Error: {e}")

    async def notify_run_completion(self, new_leads_count: int, niche: str):
        if new_leads_count > 0:
            msg = f"🚀 **Stratosphere Alert**\n\nFound **{new_leads_count}** new leads in *{niche}*.\n\nCheck Dashboard: https://stratosphereleads.up.railway.app"
            await self.send_alert(msg)
