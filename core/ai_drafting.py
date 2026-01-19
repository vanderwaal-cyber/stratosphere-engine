import urllib.request
import urllib.error
import json
import os
from typing import Dict, Any

# Ensure you export OPENAI_API_KEY="sk-..."
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

class DMDrafter:
    def __init__(self, api_key: str = None):
        self.api_key = api_key or OPENAI_API_KEY
        self.url = "https://api.openai.com/v1/chat/completions"

    def _call_openai(self, prompt: str) -> str:
        """Calls OpenAI API using standard library to avoid dependencies."""
        if not self.api_key:
            return "[DRAFT] (No OpenAI Key provided. Export OPENAI_API_KEY to generate real drafts.)"

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        
        payload = {
            "model": "gpt-4o",
            "messages": [
                {"role": "system", "content": "You are 0x_degenola, a BD lead at Stratosphere (a premiere Telegram Marketing agency for Web3). Write short, high-status, personalized DMs."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 150,
            "temperature": 0.7
        }

        try:
            req = urllib.request.Request(
                self.url, 
                data=json.dumps(payload).encode('utf-8'), 
                headers=headers
            )
            with urllib.request.urlopen(req) as response:
                if response.status == 200:
                    result = json.loads(response.read().decode('utf-8'))
                    return result['choices'][0]['message']['content'].strip()
                return "[Error] OpenAI returned non-200 status."
        except urllib.error.HTTPError as e:
            print(f"OpenAI API Error: {e.code} - {e.reason}")
            return f"[Error] OpenAI API Error: {e.code}"
        except Exception as e:
            print(f"Details: {e}")
            return f"[Error] {str(e)}"

    def generate_draft(self, project: Dict[str, Any]) -> str:
        """
        Generates a personalized DM based on project info.
        """
        name = project.get('project_name', 'your project')
        desc = project.get('description', 'a generic web3 project')
        
        context = f"Project: {name}\nDescription: {desc}"
        
        # --- FALLBACK: TEMPLATE MODE (No API Key) ---
        if not self.api_key:
            return (
                f"Hey! 👋 0x_degenola here from Stratosphere.\n\n"
                f"Just saw {name} pop up on the radar. "
                "We help projects amplify their TG presence heavily.\n\n"
                "Would love to chat if you're looking to scale the community."
            )
        
        prompt = (
            f"Here is a crypto project I want to reach out to:\n{context}\n\n"
            "Draft a very short (max 280 chars), casual first DM from me (0x_degenola).\n"
            "Goal: Start a conversation about how Stratosphere can amplify their Telegram presence/marketing.\n"
            "Constraint: NO generic sales pitch. Reference their specific project details to show I did research.\n"
            "Tone: Degen-friendly, casual, concise."
        )
        
        return self._call_openai(prompt)

    def generate_analysis(self, project: Dict[str, Any]) -> Dict[str, str]:
        """
        Generates a 3-part analysis: Strategy, Tech Stack, and Icebreaker.
        Returns a dictionary.
        """
        name = project.get('project_name', 'This project')
        desc = project.get('description', '')
        
        if not self.api_key:
            # Fallback Mock Analysis
            return {
                "ai_analysis": f"🚀 **Launch Phase**: {name} appears to be early stage.\n🛠 **Tech**: Detected Web3 keyword patterns.",
                "icebreaker": self.generate_draft(project)
            }
            
        prompt = (
            f"Analyze this crypto project based on its description:\n"
            f"Project: {name}\nDescription: {desc}\n\n"
            "Output a JSON object with 2 keys:\n"
            "1. 'analysis': A generic growth strategy assessment (bullet points with emojis). Mention the stage (Launch/Scaling) and Tech Stack inferred.\n"
            "2. 'icebreaker': A very casual, short (under 280 chars) DM opener to the founder. No corporate jargon. 'Degen' friendly.\n\n"
            "Example JSON format:\n"
            "{\"analysis\": \"🚀 **Pre-Launch**: ...\", \"icebreaker\": \"Yo...\"}"
        )
        
        try:
            # We urge GPT to return JSON by prompting it, but we'll need to parse strictly
            res = self._call_openai(prompt + "\n\nRespond ONLY with the JSON.")
            # Simple cleanup for markdown code blocks if GPT adds them
            res = res.replace("```json", "").replace("```", "").strip()
            data = json.loads(res)
            return {
                "ai_analysis": data.get("analysis", "Analysis failed to parse."),
                "icebreaker": data.get("icebreaker", "Hey, saw the project!")
            }
        except Exception:
            # Fallback if JSON parsing fails
            return {
                "ai_analysis": "⚠️ AI Parsing Error (Raw Output): " + res if 'res' in locals() else "Error",
                "icebreaker": self.generate_draft(project)
            }
