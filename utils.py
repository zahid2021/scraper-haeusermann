import os
from groq import Groq
from dotenv import load_dotenv
import json

load_dotenv()

def build_job_parser_prompt(job_description: str) -> str:
    prompt = f"""
            You are an expert job data extraction AI for German-speaking job markets (DE, AT, CH).

            Your task is to extract structured job information from raw job descriptions.

            ====================
            CRITICAL OUTPUT RULES
            ====================
            - Return ONLY valid JSON
            - No explanations
            - No markdown
            - Follow schema EXACTLY
            - Use "" or [] or 0 if missing
            - Do NOT invent data
            - Normalize enum values exactly as specified
            - Skills must be lowercase
            - Remove duplicates
            - Summary max 3 sentences in German

            ====================
            ENUM NORMALIZATION
            ====================

            seniority_level:
            intern, junior, mid, senior, lead, manager

            employment_type:
            full-time, part-time, contract, internship, temporary

            remote_type:
            on-site, hybrid, remote

            company_type:
            employer, recruiter

            company_size:
            micro, small, medium, large, enterprise

            ====================
            WORKLOAD RULES
            ====================
            - Extract percentage if mentioned (e.g. 80–100%)
            - workload_min = lowest %
            - workload_max = highest %
            - If not mentioned → 0

            ====================
            EXPERIENCE RULES
            ====================
            - Extract min/max years if range exists
            - If "mehrjährige Erfahrung" → 3 minimum
            - If unclear → 0

            ====================
            MANAGEMENT RULES
            ====================
            management_responsibility = true if:
            - Teamleitung
            - Führungsverantwortung
            - Leitung
            - Head of
            Otherwise false

            home_office_possible = true if:
            - Homeoffice
            - Hybrid
            - Remote
            Otherwise false

            ====================
            OUTPUT JSON SCHEMA
            ====================

            {{
              "title": "",
              "summary": "",

              "company": {{
                "name": "",
                "industry": "",
                "company_type": "",
                "company_size": ""
              }},

              "category": {{
                "main_category": "",
                "sub_category": ""
              }},

              "location": {{
                "country": "",
                "state": "",
                "city": "",
                "postal_code": ""
              }},

              "seniority_level": "",
              "experience_min_years": 0,
              "experience_max_years": 0,

              "employment_type": "",
              "workload_min": 0,
              "workload_max": 0,

              "remote_type": "",
              "management_responsibility": false,
              "home_office_possible": false,

              "education_level": "",
              "languages": [],

              "required_skills": [],
              "preferred_skills": [],

              "published_at": ""
            }}

            ====================
            JOB DESCRIPTION
            ====================

            {job_description}
                
                """
    return prompt

def generate(input):
    prompt = build_job_parser_prompt(input)
    client = Groq(api_key=os.getenv("GROQ_API_KEY"))

    response = client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[{"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content

def parse_llm_response(llm_response: str) -> dict:
    try:
        cleaned = llm_response.strip()
        if cleaned.startswith('```'):
            cleaned = cleaned.split('\n', 1)[1] if '\n' in cleaned else cleaned
            if cleaned.endswith('```'):
                cleaned = cleaned.rsplit('```', 1)[0]
        
        return json.loads(cleaned.strip())
    except json.JSONDecodeError as e:
        print(f"✗ Failed to parse LLM response: {e}")
