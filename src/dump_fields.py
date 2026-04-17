"""Pipedrive dealFields를 빠르게 덤프 (deal 동기화 없이 field 구조만).

실패 사유·이탈 원인 등 저장된 custom field 탐색용.
"""
import os
import sys
import json
import urllib.request
import urllib.parse
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def fetch_fields(token, domain, label):
    url = f"https://{domain}.pipedrive.com/api/v1/dealFields?api_token={token}&limit=500"
    print(f"[{label}] {url.split('?')[0]}")
    req = urllib.request.Request(url, headers={"User-Agent": "bznav-refund/1.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    fields = data.get("data") or []
    print(f"[{label}] fetched {len(fields)} fields")
    return fields


def summarize(fields, source_label):
    result = []
    for f in fields:
        name = f.get("name", "")
        ftype = f.get("field_type", "")
        options = f.get("options") or []
        result.append({
            "source": source_label,
            "key": f.get("key"),
            "name": name,
            "field_type": ftype,
            "is_custom": f.get("edit_flag", False),
            "options_count": len(options),
            "options": [o.get("label", "") for o in options[:30]],
        })
    return result


def main():
    token_indiv = os.environ.get("PIPEDRIVE_API_TOKEN")
    domain_indiv = os.environ.get("PIPEDRIVE_DOMAIN")
    token_corp = os.environ.get("CORP_PIPEDRIVE_API_TOKEN")
    domain_corp = os.environ.get("CORP_PIPEDRIVE_DOMAIN", domain_indiv)

    all_fields = []
    if token_indiv and domain_indiv:
        all_fields += summarize(fetch_fields(token_indiv, domain_indiv, "개인"), "개인")
    if token_corp and domain_corp:
        all_fields += summarize(fetch_fields(token_corp, domain_corp, "법인"), "법인")

    if not all_fields:
        print("ERROR: no tokens", file=sys.stderr)
        sys.exit(1)

    # 키워드 필터
    keywords = ["이탈", "사유", "실패", "reason", "lost", "이유", "원인", "취소", "이관", "감면"]
    flagged = [f for f in all_fields if any(k in f["name"].lower() or k in f["name"] for k in keywords)]

    out = {
        "total_fields": len(all_fields),
        "flagged_for_reason_analysis": flagged,
        "all_fields": all_fields,
    }

    out_path = OUTPUT_DIR / "field_catalog.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2))
    print(f"\n→ {out_path}")
    print(f"   total: {len(all_fields)}, flagged: {len(flagged)}")
    print(f"\n키워드 매칭 필드:")
    for f in flagged:
        opts = f", options: {f['options'][:3]}..." if f["options_count"] > 0 else ""
        print(f"  [{f['source']}] {f['name']} (type={f['field_type']}, key={f['key']}{opts})")


if __name__ == "__main__":
    main()
