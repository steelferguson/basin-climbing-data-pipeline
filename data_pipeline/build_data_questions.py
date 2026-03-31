"""
Build data questions — customers where we need human help.

Identifies customers with ambiguous or missing data that the pipeline
can't resolve automatically. These show up in the CRM as a queue for
crew to review and fix.

Output: customers/data_questions.csv in S3
"""

import os
import pandas as pd
import boto3
from io import StringIO
from dotenv import load_dotenv

load_dotenv()


def build_data_questions() -> pd.DataFrame:
    """Identify customers with data questions."""

    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        config=boto3.session.Config(read_timeout=120)
    )
    bucket = "basin-climbing-data-prod"

    def load_s3(key):
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            return pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        except Exception:
            return pd.DataFrame()

    print("=" * 60)
    print("BUILDING DATA QUESTIONS")
    print("=" * 60)

    df_cm = load_s3('customers/customer_master_v2.csv')
    df_family = load_s3('customers/family_relationships.csv')
    df_checkins = load_s3('capitan/checkins.csv')

    if df_cm.empty:
        return pd.DataFrame()

    df_cm['customer_id'] = df_cm['customer_id'].astype(str)
    linked_children = set(df_family['child_customer_id'].astype(float).astype(int).astype(str)) if not df_family.empty else set()

    # Prep checkin dates
    if not df_checkins.empty:
        df_checkins['checkin_datetime'] = pd.to_datetime(df_checkins['checkin_datetime'], errors='coerce')
        df_checkins['checkin_date'] = df_checkins['checkin_datetime'].dt.date
        df_checkins['customer_id'] = df_checkins['customer_id'].astype(str)

    adults_with_email = df_cm[(~df_cm['is_child']) & (df_cm['email'].notna()) & (df_cm['email'] != '')]
    adults_with_email = adults_with_email.copy()
    adults_with_email['last_lower'] = adults_with_email['last_name'].str.lower().str.strip()

    questions = []

    # =========================================================
    # Q1: Active children with no parent link and no email
    # =========================================================
    print("\n🔍 Q1: Children with no parent link...")

    unlinked_children = df_cm[
        (df_cm['is_child'] == True) &
        (~df_cm['customer_id'].isin(linked_children)) &
        (df_cm['contact_email'].isna()) &
        (df_cm['total_visits'] > 0)
    ]

    for _, child in unlinked_children.iterrows():
        child_last = str(child.get('last_name', '')).lower().strip()
        matching_adults = adults_with_email[adults_with_email['last_lower'] == child_last]

        if len(matching_adults) == 0:
            questions.append({
                'customer_id': child['customer_id'],
                'customer_name': f"{child['first_name']} {child['last_name']}",
                'question_type': 'missing_parent',
                'question': f"Child with {int(child['total_visits'])} visits but no parent link and no matching adult last name. Who is their parent/guardian?",
                'priority': 'high' if child['total_visits'] >= 3 else 'medium',
                'context': f"Age: {child.get('age', '?')}, Last visit: {child.get('last_checkin', 'unknown')}",
            })
        elif len(matching_adults) >= 2:
            adult_names = ', '.join(
                f"{r['first_name']} {r['last_name']}" for _, r in matching_adults.head(5).iterrows()
            )
            questions.append({
                'customer_id': child['customer_id'],
                'customer_name': f"{child['first_name']} {child['last_name']}",
                'question_type': 'ambiguous_parent',
                'question': f"Child has {len(matching_adults)} adults with last name '{child['last_name']}'. Which is the parent?",
                'priority': 'medium',
                'context': f"Possible parents: {adult_names}",
            })

    print(f"  Found {len([q for q in questions if q['question_type'] in ('missing_parent', 'ambiguous_parent')])} parent questions")

    # =========================================================
    # Q2: Leads with email but not in any Klaviyo flow
    # =========================================================
    print("\n🔍 Q2: Reachable leads not in Klaviyo...")

    reachable_leads = df_cm[
        (df_cm['is_lead'] == True) &
        (df_cm['contact_email'].notna()) &
        (df_cm['klaviyo_emails_received'] == 0) &
        (df_cm['total_visits'] >= 3)
    ]

    for _, lead in reachable_leads.head(50).iterrows():  # Cap at 50
        questions.append({
            'customer_id': lead['customer_id'],
            'customer_name': f"{lead['first_name']} {lead['last_name']}",
            'question_type': 'no_klaviyo',
            'question': f"Has {int(lead['total_visits'])} visits and email but never entered a Klaviyo flow. Why?",
            'priority': 'high' if lead['total_visits'] >= 5 else 'medium',
            'context': f"Email: {lead['contact_email']}, Lead source: {lead.get('lead_source', '?')}",
        })

    print(f"  Found {len([q for q in questions if q['question_type'] == 'no_klaviyo'])} Klaviyo gap questions")

    # =========================================================
    # Q3: Lapsed members who haven't been contacted
    # =========================================================
    print("\n🔍 Q3: Lapsed members not contacted...")

    lapsed_no_contact = df_cm[
        (df_cm['is_lapsed_member'] == True) &
        (~df_cm['has_been_contacted']) &
        (df_cm['contact_email'].notna())
    ]

    for _, lapsed in lapsed_no_contact.iterrows():
        questions.append({
            'customer_id': lapsed['customer_id'],
            'customer_name': f"{lapsed['first_name']} {lapsed['last_name']}",
            'question_type': 'lapsed_no_contact',
            'question': f"Lapsed member (was: {lapsed.get('membership_name', '?')}). Never contacted by crew. Should we reach out?",
            'priority': 'medium',
            'context': f"Email: {lapsed['contact_email']}, Last visit: {lapsed.get('last_checkin', 'unknown')}",
        })

    print(f"  Found {len([q for q in questions if q['question_type'] == 'lapsed_no_contact'])} lapsed member questions")

    df_questions = pd.DataFrame(questions)

    if not df_questions.empty:
        df_questions = df_questions.sort_values(
            ['priority', 'question_type'],
            key=lambda x: x.map({'high': 0, 'medium': 1, 'low': 2}) if x.name == 'priority' else x
        )

    print(f"\n{'=' * 60}")
    print(f"DATA QUESTIONS SUMMARY")
    print(f"{'=' * 60}")
    print(f"Total questions: {len(df_questions)}")
    if not df_questions.empty:
        print(f"By type:")
        print(df_questions['question_type'].value_counts().to_string())
        print(f"By priority:")
        print(df_questions['priority'].value_counts().to_string())

    return df_questions


def upload_data_questions():
    """Build and upload data questions."""
    df = build_data_questions()

    if df.empty:
        print("No questions to upload")
        return df

    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )

    csv_buf = StringIO()
    df.to_csv(csv_buf, index=False)
    s3.put_object(
        Bucket="basin-climbing-data-prod",
        Key="customers/data_questions.csv",
        Body=csv_buf.getvalue()
    )
    print(f"\n✅ Uploaded {len(df)} questions to customers/data_questions.csv")
    return df


if __name__ == "__main__":
    upload_data_questions()
