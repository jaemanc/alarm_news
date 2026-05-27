#!/usr/bin/env python3
"""
대화형 사용자 등록 스크립트 (Interactive User Registration)

터미널에서 사용자와 대화하며:
1. 키워드 수집 (주식/뉴스 관련, 최소 5개 ~ 최대 10개)
2. 이메일 + 알림 시간 수집 (하루 1회)
3. 입력 정보 확인
4. 더미 데이터로 미리보기 이메일 전송

사용법:
    python scripts/interactive_register.py
"""
import re
import smtplib
import os
import sys
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Optional, Tuple

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# .env 파일 로드
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

# ─── 설정 ───────────────────────────────────────────────────────────────────

EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
MIN_KEYWORDS = 5
MAX_KEYWORDS = 10

# SMTP 설정 (환경변수에서 읽거나 기본값 사용)
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USERNAME = os.environ.get("SMTP_USERNAME", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
SMTP_FROM_EMAIL = os.environ.get("SMTP_FROM_EMAIL", "noreply@alarmnews.com")

# Resend API (SMTP 대안 - https://resend.com)
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")


# ─── 더미 데이터 ────────────────────────────────────────────────────────────

DUMMY_NEWS = [
    {"title": "AI 반도체 수요 급증, NVIDIA 실적 전망 상향", "summary": "글로벌 AI 투자 확대로 반도체 수요가 급증하며 NVIDIA의 분기 실적이 시장 예상을 크게 상회할 것으로 전망됩니다.", "url": "https://example.com/ai-nvidia"},
    {"title": "테슬라, 자율주행 FSD v13 업데이트 발표", "summary": "테슬라가 완전자율주행 소프트웨어의 대규모 업데이트를 발표하며 주가가 장중 5% 상승했습니다.", "url": "https://example.com/tesla-fsd"},
    {"title": "한국은행 기준금리 동결, 하반기 인하 시사", "summary": "한국은행이 기준금리를 3.0%로 동결하면서도 하반기 인하 가능성을 열어두었습니다.", "url": "https://example.com/bok-rate"},
]

DUMMY_STOCKS = [
    {"symbol": "NVDA", "company": "NVIDIA Corp.", "price": 1247.50, "change": +3.21},
    {"symbol": "TSLA", "company": "Tesla Inc.", "price": 285.30, "change": +5.12},
    {"symbol": "005930.KS", "company": "삼성전자", "price": 78500, "change": -0.85},
]


# ─── 유틸리티 ────────────────────────────────────────────────────────────────

def validate_email(email: str) -> bool:
    """이메일 유효성 검사."""
    return EMAIL_REGEX.match(email.strip()) is not None


def validate_time(time_str: str) -> Optional[Tuple[int, int]]:
    """시간 문자열 (HH:MM) 파싱. 유효하면 (hour, minute) 반환."""
    try:
        parts = time_str.strip().split(":")
        if len(parts) != 2:
            return None
        hour, minute = int(parts[0]), int(parts[1])
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return (hour, minute)
        return None
    except (ValueError, IndexError):
        return None


def extract_keywords(user_input: str) -> List[str]:
    """사용자 입력에서 키워드 추출 (다양한 구분자 지원)."""
    # 다양한 쉼표/구분자를 통일 (전각쉼표, 중점, 세미콜론 등)
    normalized = user_input
    for sep in ["，", "、", "；", ";", "|", "/"]:
        normalized = normalized.replace(sep, ",")
    
    # 쉼표로 분리
    if "," in normalized:
        keywords = [k.strip() for k in normalized.split(",")]
    else:
        # 쉼표 없으면 공백 2개 이상으로 분리 시도, 아니면 전체를 하나로
        parts = re.split(r'\s{2,}', normalized.strip())
        if len(parts) > 1:
            keywords = [k.strip() for k in parts]
        else:
            # 마지막 수단: 공백으로 분리
            keywords = normalized.strip().split()
    
    # 빈 문자열 제거, 중복 제거 (순서 유지)
    seen = set()
    result = []
    for k in keywords:
        k = k.strip()
        if k and k.lower() not in seen:
            seen.add(k.lower())
            result.append(k)
    return result


def build_preview_html(keywords: List[str], email: str, time_str: str) -> str:
    """더미 데이터로 미리보기 이메일 HTML 생성."""
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")

    # 키워드와 매칭되는 더미 뉴스/주식 필터 (전부 보여줌 - 더미니까)
    news_html = ""
    for article in DUMMY_NEWS:
        news_html += f"""
        <tr>
            <td style="padding:8px; border-bottom:1px solid #eee;">
                <a href="{article['url']}" style="color:#1a73e8; text-decoration:none; font-weight:bold;">{article['title']}</a>
                <br><span style="color:#555; font-size:13px;">{article['summary']}</span>
            </td>
        </tr>"""

    stocks_html = ""
    for stock in DUMMY_STOCKS:
        change_color = "#16a34a" if stock["change"] >= 0 else "#dc2626"
        change_sign = "+" if stock["change"] >= 0 else ""
        stocks_html += f"""
        <tr>
            <td style="padding:6px 12px; border-bottom:1px solid #eee; font-weight:bold;">{stock['symbol']}</td>
            <td style="padding:6px 12px; border-bottom:1px solid #eee;">{stock['company']}</td>
            <td style="padding:6px 12px; border-bottom:1px solid #eee; text-align:right;">{stock['price']:,.2f}</td>
            <td style="padding:6px 12px; border-bottom:1px solid #eee; text-align:right; color:{change_color};">{change_sign}{stock['change']:.2f}%</td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width:600px; margin:0 auto; padding:20px;">
    <div style="background:#1a73e8; color:white; padding:20px; border-radius:8px 8px 0 0;">
        <h1 style="margin:0; font-size:22px;">📰 Alarm News</h1>
        <p style="margin:5px 0 0; opacity:0.9;">{date_str} 일일 브리핑</p>
    </div>

    <div style="border:1px solid #e0e0e0; border-top:none; padding:20px; border-radius:0 0 8px 8px;">
        <p>안녕하세요! 설정하신 키워드에 맞는 오늘의 뉴스와 주식 정보입니다.</p>
        <p style="color:#666; font-size:13px;">키워드: <strong>{', '.join(keywords)}</strong> | 알림시간: <strong>{time_str}</strong></p>

        <h2 style="color:#1a73e8; border-bottom:2px solid #1a73e8; padding-bottom:8px;">📋 뉴스</h2>
        <table style="width:100%; border-collapse:collapse;">
            {news_html}
        </table>

        <h2 style="color:#1a73e8; border-bottom:2px solid #1a73e8; padding-bottom:8px; margin-top:24px;">📈 주식</h2>
        <table style="width:100%; border-collapse:collapse;">
            <tr style="background:#f5f5f5;">
                <th style="padding:8px 12px; text-align:left;">종목</th>
                <th style="padding:8px 12px; text-align:left;">회사명</th>
                <th style="padding:8px 12px; text-align:right;">현재가</th>
                <th style="padding:8px 12px; text-align:right;">변동</th>
            </tr>
            {stocks_html}
        </table>

        <hr style="margin:24px 0; border:none; border-top:1px solid #eee;">
        <p style="color:#999; font-size:12px;">
            ⚠️ 이 메일은 <strong>미리보기 테스트</strong>입니다. 실제 서비스 등록 후 매일 {time_str}에 발송됩니다.<br>
            생성시각: {now.isoformat()}Z<br>
            구독 해지: 계정 설정에서 구독을 취소하거나 support@alarmnews.com으로 문의하세요.
        </p>
    </div>
</body>
</html>"""
    return html


def send_preview_email(to_email: str, keywords: List[str], time_str: str) -> bool:
    """미리보기 이메일 전송. 우선순위: Resend API → SMTP → 파일 저장 + 브라우저 열기."""
    subject = f"[미리보기] Alarm News - {datetime.now().strftime('%Y-%m-%d')} - {', '.join(keywords[:3])}..."
    body_html = build_preview_html(keywords, to_email, time_str)

    # 방법 1: Resend API (SMTP 불필요, API 키만 있으면 됨)
    if RESEND_API_KEY:
        return _send_via_resend(to_email, subject, body_html)

    # 방법 2: SMTP
    if SMTP_USERNAME and SMTP_PASSWORD:
        return _send_via_smtp(to_email, subject, body_html)

    # 방법 3: 파일 저장 + 브라우저에서 열기
    return _save_and_open_preview(body_html)


def _send_via_resend(to_email: str, subject: str, body_html: str) -> bool:
    """Resend API로 이메일 전송 (https://resend.com - 무료 월 100통)."""
    try:
        import urllib.request
        import json

        data = json.dumps({
            "from": "Alarm News <onboarding@resend.dev>",
            "to": [to_email],
            "subject": subject,
            "html": body_html,
        }).encode("utf-8")

        req = urllib.request.Request(
            "https://api.resend.com/emails",
            data=data,
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json",
            },
        )

        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                print(f"\n  ✅ Resend API로 미리보기 이메일을 {to_email}로 전송했습니다!")
                return True
            else:
                print(f"\n  ⚠️  Resend API 응답: {resp.status}")
                return _save_and_open_preview(body_html)
    except Exception as e:
        print(f"\n  ⚠️  Resend API 전송 실패: {e}")
        return _save_and_open_preview(body_html)


def _send_via_smtp(to_email: str, subject: str, body_html: str) -> bool:
    """SMTP로 이메일 전송."""
    try:
        msg = MIMEMultipart("alternative")
        msg["From"] = f"Alarm News <{SMTP_FROM_EMAIL}>"
        msg["To"] = to_email
        msg["Subject"] = subject
        msg.attach(MIMEText(body_html, "html", "utf-8"))

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as server:
            server.starttls()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.sendmail(SMTP_FROM_EMAIL, to_email, msg.as_string())

        print(f"\n  ✅ SMTP로 미리보기 이메일을 {to_email}로 전송했습니다!")
        return True
    except Exception as e:
        print(f"\n  ⚠️  SMTP 전송 실패: {e}")
        return _save_and_open_preview(body_html)


def _save_and_open_preview(body_html: str) -> bool:
    """HTML 파일로 저장하고 브라우저에서 자동으로 열기."""
    import subprocess
    import platform

    output_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "preview_email.html"
    )
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(body_html)

    print(f"\n  💾 이메일 전송 수단 미설정 → 미리보기를 파일로 저장했습니다.")
    print(f"     📄 {output_path}")

    # macOS에서 자동으로 브라우저 열기
    if platform.system() == "Darwin":
        try:
            subprocess.run(["open", output_path], check=True)
            print(f"     🌐 브라우저에서 열었습니다!")
        except Exception:
            pass
    elif platform.system() == "Linux":
        try:
            subprocess.run(["xdg-open", output_path], check=True)
        except Exception:
            pass

    print()
    print("  💡 실제 이메일 전송을 원하면:")
    print("     방법 1 (추천): RESEND_API_KEY 환경변수 설정")
    print("            → https://resend.com 에서 무료 API 키 발급")
    print("     방법 2: SMTP_USERNAME + SMTP_PASSWORD 환경변수 설정")
    print("            → Gmail 앱 비밀번호 사용")
    return True


# ─── 메인 대화 흐름 ──────────────────────────────────────────────────────────

def main():
    print()
    print("=" * 60)
    print("  📰 Alarm News - 사용자 등록")
    print("=" * 60)
    print()
    print("  매일 원하는 시간에 관심 키워드의 뉴스와 주식 정보를")
    print("  이메일로 받아보세요!")
    print()

    # ─── Step 1: 키워드 수집 ─────────────────────────────────────────────────
    print("─" * 60)
    print("  📌 Step 1: 관심 키워드 입력")
    print(f"     (주식/뉴스 관련, 최소 {MIN_KEYWORDS}개 ~ 최대 {MAX_KEYWORDS}개)")
    print("     예: NVIDIA, 테슬라, AI, 반도체, 금리")
    print("─" * 60)

    keywords: List[str] = []
    while True:
        user_input = input("\n  키워드를 입력하세요 (쉼표로 구분): ").strip()
        if not user_input:
            print("  ⚠️  키워드를 입력해주세요.")
            continue

        keywords = extract_keywords(user_input)
        print(f"  → 인식된 키워드: {keywords}")

        if len(keywords) < MIN_KEYWORDS:
            print(f"  ⚠️  최소 {MIN_KEYWORDS}개 필요합니다. (현재 {len(keywords)}개: {', '.join(keywords)})")
            print(f"     더 추가해주세요.")
            more = input("  추가 키워드: ").strip()
            if more:
                keywords.extend(extract_keywords(more))
                # 중복 제거
                seen = set()
                unique = []
                for k in keywords:
                    if k.lower() not in seen:
                        seen.add(k.lower())
                        unique.append(k)
                keywords = unique

            if len(keywords) < MIN_KEYWORDS:
                print(f"  ⚠️  아직 {len(keywords)}개입니다. 다시 입력해주세요.")
                keywords = []
                continue

        if len(keywords) > MAX_KEYWORDS:
            keywords = keywords[:MAX_KEYWORDS]
            print(f"  ℹ️  최대 {MAX_KEYWORDS}개까지만 사용합니다.")

        print(f"\n  ✅ 키워드 ({len(keywords)}개): {', '.join(keywords)}")
        confirm = input("  이대로 진행할까요? (Y/n): ").strip().lower()
        if confirm in ("", "y", "yes", "ㅛ"):
            break
        else:
            keywords = []
            print("  다시 입력해주세요.")

    # ─── Step 2: 이메일 + 시간 ───────────────────────────────────────────────
    print()
    print("─" * 60)
    print("  📧 Step 2: 이메일 주소 & 알림 시간")
    print("─" * 60)

    email = ""
    while True:
        email = input("\n  이메일 주소: ").strip()
        if validate_email(email):
            break
        print("  ⚠️  유효하지 않은 이메일 형식입니다. 다시 입력해주세요.")

    time_str = ""
    notification_time: Tuple[int, int] = (9, 0)
    print("\n  알림 시간을 설정하세요 (하루 1회, 24시간 형식)")
    print("  예: 09:00, 18:30, 07:00")
    while True:
        time_str = input("  알림 시간 (HH:MM): ").strip()
        result = validate_time(time_str)
        if result:
            notification_time = result
            time_str = f"{result[0]:02d}:{result[1]:02d}"
            break
        print("  ⚠️  올바른 형식이 아닙니다. HH:MM (예: 09:00)")

    # ─── Step 3: 확인 ────────────────────────────────────────────────────────
    print()
    print("─" * 60)
    print("  📋 Step 3: 입력 정보 확인")
    print("─" * 60)
    print()
    print(f"  📧 이메일:    {email}")
    print(f"  ⏰ 알림시간:  매일 {time_str}")
    print(f"  🔑 키워드:    {', '.join(keywords)}")
    print()

    while True:
        confirm = input("  이 정보로 등록하시겠습니까? (Y/n): ").strip().lower()
        if confirm in ("", "y", "yes", "ㅛ"):
            break
        elif confirm in ("n", "no", "ㅜ"):
            print("\n  등록을 취소합니다.")
            return
        else:
            print("  Y 또는 N을 입력해주세요.")

    # ─── Step 4: 미리보기 이메일 전송 ────────────────────────────────────────
    print()
    print("─" * 60)
    print("  📨 Step 4: 미리보기 이메일 전송")
    print("─" * 60)
    print()
    print("  더미 데이터로 미리보기 이메일을 생성합니다...")

    send_preview_email(email, keywords, time_str)

    # ─── 완료 ────────────────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("  🎉 등록 완료!")
    print("=" * 60)
    print()
    print(f"  매일 {time_str}에 '{', '.join(keywords[:3])}...' 관련")
    print(f"  뉴스와 주식 정보를 {email}로 보내드립니다.")
    print()
    print("  ※ 현재는 테스트 모드입니다.")
    print("     실제 서비스를 위해서는 SMTP 환경변수를 설정하세요:")
    print("     SMTP_HOST, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD")
    print()


if __name__ == "__main__":
    main()
