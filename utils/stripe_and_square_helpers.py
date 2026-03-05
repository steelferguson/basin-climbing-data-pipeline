import re
import pandas as pd
from data_pipeline import config


def extract_event_and_programming_subcategory(description):
    if ":" in description:
        after_colon = description.split(":", 1)[1]
    else:
        after_colon = description
    cleaned = re.sub(r"[^A-Za-z\s]", "", after_colon)
    cleaned = cleaned.strip().lower()
    return cleaned


def get_unique_event_and_programming_subcategories(
    df,
    category_col="revenue_category",
    subcat_col="sub_category",
    desc_col="Description",
):
    mask = df[category_col].str.lower().isin(["event booking", "programming"]) & (
        df[subcat_col] != "birthday"
    )
    subcats = df.loc[mask, desc_col].apply(extract_event_and_programming_subcategory)
    return sorted(set(subcats))


def categorize_day_pass_sub_category(description, age_keywords, gear_keywords):
    description = description.lower()
    age_sub_category = ""
    gear_sub_category = ""
    for keyword, sub_category in age_keywords.items():
        if keyword in description:
            age_sub_category = sub_category
    for keyword, sub_category in gear_keywords.items():
        if keyword in description:
            gear_sub_category = sub_category
    return (age_sub_category + " " + gear_sub_category).strip()


def get_unique_day_pass_subcategories(df, age_keywords, gear_keywords):
    mask = df["revenue_category"].str.contains("Day Pass", case=False, na=False)
    subcats = df.loc[mask, "Description"].apply(
        lambda desc: categorize_day_pass_sub_category(desc, age_keywords, gear_keywords)
    )
    return sorted(set(subcats))


def categorize_transaction(
    description,
    revenue_category_keywords,
    membership_size_keywords,
    membership_frequency_keywords,
    founder_keywords,
    bcf_fam_friend_keywords,
):
    description = description.lower()
    category = "Retail"
    membership_size = None
    membership_freq = None
    is_founder = False
    is_bcf_staff_or_friend = False

    for keyword, cat in revenue_category_keywords.items():
        if keyword in description:
            category = cat
            break

    for keyword, mem_size in membership_size_keywords.items():
        if keyword in description:
            membership_size = mem_size
            break

    for keyword, mem_freq in membership_frequency_keywords.items():
        if keyword in description:
            membership_freq = mem_freq
            break

    if any(keyword in description for keyword in founder_keywords):
        is_founder = True

    if any(keyword in description for keyword in bcf_fam_friend_keywords):
        is_bcf_staff_or_friend = True

    return (
        category,
        membership_size,
        membership_freq,
        is_founder,
        is_bcf_staff_or_friend,
    )


def transform_payments_data(
    df,
    assign_extra_subcategories=None,  # Optional callback for pipeline-specific logic
    data_source_name=None,
    day_pass_count_logic=None,  # Optional callback for day pass count
):
    """
    Shared transformation logic for Stripe and Square payments data.
    """
    # Categorize transactions
    df[
        [
            "revenue_category",
            "membership_size",
            "membership_freq",
            "is_founder",
            "is_free_membership",
        ]
    ] = df["Description"].apply(
        lambda x: pd.Series(
            categorize_transaction(
                x,
                config.revenue_category_keywords,
                config.membership_size_keywords,
                config.membership_frequency_keywords,
                config.founder_keywords,
                config.bcf_fam_friend_keywords,
            )
        )
    )

    # Add sub-category columns
    df["sub_category"] = ""
    df["sub_category_detail"] = ""

    # Camps
    df.loc[
        df["Description"].str.contains("Summer Camp", case=False, na=False),
        "sub_category",
    ] = "camps"
    # Extract session number OR "BONUS WEEK"
    df.loc[
        df["Description"].str.contains("Summer Camp", case=False, na=False),
        "sub_category_detail",
    ] = df["Description"].str.extract(r"(Summer Camp (?:Session \d+|BONUS WEEK))", expand=False, flags=re.IGNORECASE)

    # Birthday
    for pattern, detail in config.birthday_sub_category_patterns.items():
        mask = df["Description"].str.contains(pattern, case=False, na=False)
        df.loc[mask, "sub_category"] = "birthday"
        df.loc[mask, "sub_category_detail"] = detail

    # Fitness
    for pattern, detail in config.fitness_patterns.items():
        mask = df["Description"].str.contains(pattern, case=False, na=False)
        df.loc[mask, "sub_category"] = "fitness"
        df.loc[mask, "sub_category_detail"] = detail

    # Day Passes
    mask = df["revenue_category"].str.contains("Day Pass", case=False, na=False)
    df.loc[mask, "sub_category"] = df.loc[mask, "Description"].apply(
        lambda desc: categorize_day_pass_sub_category(
            desc,
            config.day_pass_sub_category_age_keywords,
            config.day_pass_sub_category_gear_keywords,
        )
    )

    # Event/Programming subcategories
    for patern in get_unique_event_and_programming_subcategories(df):
        mask = (
            df["revenue_category"].str.lower().isin(["event booking", "programming"])
            & (df["sub_category"] != "birthday")
            & (df["sub_category"] == "")
            & (
                df["Description"]
                .apply(extract_event_and_programming_subcategory)
                .str.contains(patern, case=False, na=False)
            )
        )
        df.loc[mask, "sub_category"] = patern

    # Retail fallback
    df.loc[
        (df["revenue_category"] == "Retail") & (df["sub_category"] == ""),
        "sub_category",
    ] = df["Name"].apply(
        lambda x: " ".join(x.split()[:4]) if isinstance(x, str) else ""
    )

    # Pipeline-specific extra subcategories
    if assign_extra_subcategories:
        df = assign_extra_subcategories(df)

    # Date/time conversions
    df["date_"] = pd.to_datetime(df["Date"], errors="coerce", utc=True)
    df["Date"] = df["date_"].dt.strftime("%Y-%m-%d")

    # Amounts
    if "Tax Amount" in df.columns:
        df["Tax Amount"] = pd.to_numeric(df["Tax Amount"], errors="coerce")
    if "Pre-Tax Amount" in df.columns:
        df["Pre-Tax Amount"] = pd.to_numeric(df["Pre-Tax Amount"], errors="coerce")
    if data_source_name:
        df["Data Source"] = data_source_name

    # Day Pass Count
    if day_pass_count_logic:
        df["Day Pass Count"] = df.apply(day_pass_count_logic, axis=1)
    else:
        # Use quantity field if available (for Square), otherwise default to 1
        df["Day Pass Count"] = df.apply(
            lambda row: (
                int(row.get("quantity", 1)) if row["revenue_category"] == "Day Pass" else 0
            ),
            axis=1
        )

    return df


def calculate_fitness_amount(df):
    """
    Calculate the fitness portion of each transaction.

    Fitness revenue includes:
    1. 100% fitness memberships/passes (Fitness Only, Fitness Membership, Fitness Annual, etc.)
    2. Fitness add-ons to climbing memberships (estimate $28/month)
    3. Fitness classes (HYROX, transformation, strength, yoga, etc.)

    Returns:
        DataFrame with 'fitness_amount' column added
    """
    df = df.copy()
    df['fitness_amount'] = 0.0

    # 1. Fitness classes - full amount is fitness
    fitness_class_mask = df['sub_category'] == 'fitness'
    df.loc[fitness_class_mask, 'fitness_amount'] = df.loc[fitness_class_mask, 'Total Amount']

    # 2. 100% fitness memberships/passes - full amount is fitness
    # These are standalone fitness products (not add-ons to climbing)
    fitness_full_patterns = [
        'fitness only',
        'fitness-only',
        'fitness membership',      # New "Fitness Membership" type
        'fitness annual',          # "Fitness Annual Membership"
        'fitness unlimited',       # "Fitness Unlimited- 3 Month"
        'fitness kickstart',       # "Fitness Kickstart- 8 week"
        'fitness 5 pack',          # "Fitness 5 Pack" entry pass
    ]
    fitness_full_mask = df['Description'].str.lower().str.contains(
        '|'.join(fitness_full_patterns), na=False, regex=True
    )
    df.loc[fitness_full_mask, 'fitness_amount'] = df.loc[fitness_full_mask, 'Total Amount']

    # 3. Fitness add-ons to climbing memberships
    # These are memberships that include fitness as an addon (e.g., "Solo w/ Fitness")
    # Common pricing: ~$28/month fitness addon
    has_fitness_addon_mask = (
        df['Description'].str.contains('fitness', case=False, na=False) &
        ~fitness_full_mask &
        ~fitness_class_mask
    )

    # For fitness add-ons, estimate based on transaction amount
    for idx in df[has_fitness_addon_mask].index:
        total_amount = df.at[idx, 'Total Amount']
        if total_amount > 60:
            # Assume $28 fitness addon for regular memberships
            df.at[idx, 'fitness_amount'] = 28.0
        elif total_amount > 0:
            # If smaller amount, maybe it's just the addon - take full amount
            df.at[idx, 'fitness_amount'] = total_amount

    return df
