import os
import json
import logging
import requests
import anthropic

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

APP_INGESTION_URL = os.environ.get("APP_INGESTION_URL")
INGESTION_SECRET_KEY = os.environ.get("INGESTION_SECRET_KEY")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
ANTHROPIC_WORKSPACE_ID = os.environ.get("ANTHROPIC_WORKSPACE_ID")

if not all([APP_INGESTION_URL, INGESTION_SECRET_KEY, ANTHROPIC_API_KEY]):
    raise ValueError("Missing required environment variables: APP_INGESTION_URL, INGESTION_SECRET_KEY, or ANTHROPIC_API_KEY")

# Pass workspace header dynamically
headers = {}
if ANTHROPIC_WORKSPACE_ID:
    headers["anthropic-workspace-id"] = ANTHROPIC_WORKSPACE_ID

claude = anthropic.Anthropic(
    api_key=ANTHROPIC_API_KEY,
    default_headers=headers
)

def extract_production_data_with_claude(pdf_url):
    logging.info(f"Processing PDF via Claude from URL: {pdf_url}")
    
    prompt = f"""
    You are an oil and gas data analyst. Extract terminal and block production figures from this document URL: {pdf_url}.
    Focus on extracting data for target assets if present: OML 111, OML 65, OML 20, OML 42, OML 18, OML 11, OML 130, OML 115, OML 127, OML 54, OML 53.
    
    Return ONLY a valid JSON array of objects with no markdown formatting. Each object must have:
    - "title_number": string (e.g., "OML 111", "OML 130")
    - "period": string (formatted "YYYY-MM")
    - "oil_bpd": number or null
    - "gas_mmscf_d": number or null
    - "source_url": string (use "{pdf_url}")
    """
    
    response = claude.messages.create(
        model="claude-3-5-sonnet-latest",
        max_tokens=1500,
        messages=[{"role": "user", "content": prompt}]
    )
    
    raw_content = response.content[0].text.strip()
    if raw_content.startswith("```json"):
        raw_content = raw_content[7:-3].strip()
    elif raw_content.startswith("```"):
        raw_content = raw_content[3:-3].strip()
        
    return json.loads(raw_content)

def send_to_lovable_webhook(records):
    logging.info(f"Posting {len(records)} records to Lovable Webhook endpoint...")
    
    payload = []
    for record in records:
        payload.append({
            "target_table": "production",
            "proposed_data": record,
            "source_url": record.get("source_url")
        })
        
    headers_req = {
        "Content-Type": "application/json",
        "x-ingestion-key": INGESTION_SECRET_KEY
    }
    
    response = requests.post(APP_INGESTION_URL, json=payload, headers=headers_req)
    
    if response.status_code == 200:
        logging.info("Successfully sent records to unverified_staging via Webhook!")
    else:
        logging.error(f"Failed to post data. Status: {response.status_code}, Response: {response.text}")
        response.raise_for_status()

if __name__ == "__main__":
    target_pdf_url = "https://www.nuprc.gov.ng/wp-content/uploads/2026/04/Production_Report.pdf"
    
    try:
        parsed_records = extract_production_data_with_claude(target_pdf_url)
        if parsed_records:
            send_to_lovable_webhook(parsed_records)
        else:
            logging.warning("No records extracted from PDF.")
    except Exception as e:
        logging.error(f"Pipeline execution failed: {str(e)}")
        raise e
