"""
Create TEMP Retry Flow - Day Pass 2 Week Offer (starting at Email 2)

This flow is identical to "Day Pass -> 2 Week Pass JOURNEY 1" but:
- Starts at the conditional split after Email 1 (skipping first SMS, delay, and email)
- Triggered by the "Temp Day Pass 2wk Offer Retry" list

For customers who already received Email 1 but were incorrectly kicked out.
"""
import os
import requests
import json

try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass

KLAVIYO_API_KEY = os.getenv("KLAVIYO_PRIVATE_KEY")

# Beta revision required for flow creation
headers = {
    "Authorization": f"Klaviyo-API-Key {KLAVIYO_API_KEY}",
    "revision": "2024-10-15.pre",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# List ID for "Temp Day Pass 2wk Offer Retry" (created earlier)
LIST_ID = "VBzKEp"

# Build the retry flow - starting AFTER Email 1
# Original flow: SMS1 -> 2hr -> Email1 -> ConditionalSplit -> 1day -> SMS2 -> 3hr -> Email2 -> ...
# Retry flow: ConditionalSplit -> 1day -> SMS2 -> 3hr -> Email2 -> ...
flow_definition = {
    "data": {
        "type": "flow",
        "attributes": {
            "name": "TEMP - Day Pass 2wk Retry (Skip Email 1)",
            "definition": {
                "triggers": [
                    {
                        "type": "list",
                        "id": LIST_ID
                    }
                ],
                "profile_filter": None,
                "actions": [
                    # Start with conditional split (originally action_4)
                    {
                        "temporary_id": "action_1",
                        "type": "conditional-split",
                        "data": {
                            "profile_filter": {
                                "condition_groups": [
                                    {
                                        "conditions": [
                                            {
                                                "type": "profile-property",
                                                "property": "properties['Shopify Tags']",
                                                "filter": {
                                                    "type": "list",
                                                    "operator": "contains",
                                                    "value": "active-membership"
                                                }
                                            }
                                        ]
                                    }
                                ]
                            }
                        },
                        "links": {
                            "next_if_true": None,  # Has membership = exit
                            "next_if_false": "action_2"  # No membership = continue
                        }
                    },
                    # 1 day delay
                    {
                        "temporary_id": "action_2",
                        "type": "time-delay",
                        "data": {
                            "unit": "days",
                            "value": 1,
                            "timezone": "profile",
                            "delay_until_weekdays": ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
                        },
                        "links": {
                            "next": "action_3"
                        }
                    },
                    # SMS #2
                    {
                        "temporary_id": "action_3",
                        "type": "send-sms",
                        "data": {
                            "message": {
                                "body": "Hey there! If you're thinking about returning, the $29 two-week pass is the easiest way to explore the gym more without committing to a membership.\n\nUnlimited visits • Free rentals • Come at your pace.\n👉 Activate here:\nhttps://mcsms.io/w4kzkw\n\nSee you soon,\nThe Basin Climbing Team",
                                "shorten_links": True,
                                "add_org_prefix": True,
                                "add_info_link": True,
                                "add_opt_out_language": True,
                                "smart_sending_enabled": True,
                                "sms_quiet_hours_enabled": True,
                                "name": "Text message #2"
                            },
                            "status": "draft"
                        },
                        "links": {
                            "next": "action_4"
                        }
                    },
                    # 3 hour delay
                    {
                        "temporary_id": "action_4",
                        "type": "time-delay",
                        "data": {
                            "unit": "hours",
                            "value": 3,
                            "secondary_value": 0,
                            "timezone": "profile"
                        },
                        "links": {
                            "next": "action_5"
                        }
                    },
                    # Email #2
                    {
                        "temporary_id": "action_5",
                        "type": "send-email",
                        "data": {
                            "message": {
                                "from_email": "info@basinclimbing.com",
                                "from_label": "Basin Climbing and Fitness",
                                "reply_to_email": "info@basinclimbing.com",
                                "cc_email": None,
                                "bcc_email": None,
                                "subject_line": "Ready to come back and climb again? 🧗‍♀️",
                                "preview_text": "",
                                "template_id": "YrRm7C",
                                "smart_sending_enabled": True,
                                "name": "Email #2"
                            },
                            "status": "draft"
                        },
                        "links": {
                            "next": "action_6"
                        }
                    },
                    # 1 day delay
                    {
                        "temporary_id": "action_6",
                        "type": "time-delay",
                        "data": {
                            "unit": "days",
                            "value": 1,
                            "timezone": "profile",
                            "delay_until_weekdays": ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
                        },
                        "links": {
                            "next": "action_7"
                        }
                    },
                    # Conditional split #2
                    {
                        "temporary_id": "action_7",
                        "type": "conditional-split",
                        "data": {
                            "profile_filter": {
                                "condition_groups": [
                                    {
                                        "conditions": [
                                            {
                                                "type": "profile-property",
                                                "property": "properties['Shopify Tags']",
                                                "filter": {
                                                    "type": "list",
                                                    "operator": "contains",
                                                    "value": "active-membership"
                                                }
                                            }
                                        ]
                                    }
                                ]
                            }
                        },
                        "links": {
                            "next_if_true": None,  # Has membership = exit
                            "next_if_false": "action_8"  # No membership = continue
                        }
                    },
                    # Email #3
                    {
                        "temporary_id": "action_8",
                        "type": "send-email",
                        "data": {
                            "message": {
                                "from_email": "info@basinclimbing.com",
                                "from_label": "Basin Climbing and Fitness",
                                "reply_to_email": "info@basinclimbing.com",
                                "cc_email": None,
                                "bcc_email": None,
                                "subject_line": "Your $29 climbing pass is still available",
                                "preview_text": "",
                                "template_id": "XQ9yzb",
                                "smart_sending_enabled": True,
                                "name": "Email #3"
                            },
                            "status": "draft"
                        },
                        "links": {
                            "next": "action_9"
                        }
                    },
                    # 3 hour delay
                    {
                        "temporary_id": "action_9",
                        "type": "time-delay",
                        "data": {
                            "unit": "hours",
                            "value": 3,
                            "secondary_value": 0,
                            "timezone": "profile"
                        },
                        "links": {
                            "next": "action_10"
                        }
                    },
                    # SMS #3
                    {
                        "temporary_id": "action_10",
                        "type": "send-sms",
                        "data": {
                            "message": {
                                "body": "Quick reminder: your $29 two-week climbing pass link is still active, but expires soon. \n\nUnlimited climbs + free rentals →\nhttps://mcsms.io/0iczz4",
                                "shorten_links": True,
                                "add_org_prefix": True,
                                "add_info_link": True,
                                "add_opt_out_language": True,
                                "smart_sending_enabled": True,
                                "sms_quiet_hours_enabled": True,
                                "name": "Text message #3"
                            },
                            "status": "draft"
                        },
                        "links": {
                            "next": "action_11"
                        }
                    },
                    # 2 day delay
                    {
                        "temporary_id": "action_11",
                        "type": "time-delay",
                        "data": {
                            "unit": "days",
                            "value": 2,
                            "timezone": "profile",
                            "delay_until_weekdays": ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
                        },
                        "links": {
                            "next": "action_12"
                        }
                    },
                    # Conditional split #3
                    {
                        "temporary_id": "action_12",
                        "type": "conditional-split",
                        "data": {
                            "profile_filter": {
                                "condition_groups": [
                                    {
                                        "conditions": [
                                            {
                                                "type": "profile-property",
                                                "property": "properties['Shopify Tags']",
                                                "filter": {
                                                    "type": "list",
                                                    "operator": "contains",
                                                    "value": "active-membership"
                                                }
                                            }
                                        ]
                                    }
                                ]
                            }
                        },
                        "links": {
                            "next_if_true": None,
                            "next_if_false": "action_13"
                        }
                    },
                    # Email #4
                    {
                        "temporary_id": "action_13",
                        "type": "send-email",
                        "data": {
                            "message": {
                                "from_email": "info@basinclimbing.com",
                                "from_label": "Basin Climbing and Fitness",
                                "reply_to_email": "info@basinclimbing.com",
                                "cc_email": None,
                                "bcc_email": None,
                                "subject_line": "Quick math: come climb 3× and it's basically free",
                                "preview_text": "",
                                "template_id": "SjRTup",
                                "smart_sending_enabled": True,
                                "name": "Email #4"
                            },
                            "status": "draft"
                        },
                        "links": {
                            "next": "action_14"
                        }
                    },
                    # 1 day delay
                    {
                        "temporary_id": "action_14",
                        "type": "time-delay",
                        "data": {
                            "unit": "days",
                            "value": 1,
                            "timezone": "profile",
                            "delay_until_weekdays": ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
                        },
                        "links": {
                            "next": "action_15"
                        }
                    },
                    # Conditional split #4
                    {
                        "temporary_id": "action_15",
                        "type": "conditional-split",
                        "data": {
                            "profile_filter": {
                                "condition_groups": [
                                    {
                                        "conditions": [
                                            {
                                                "type": "profile-property",
                                                "property": "properties['Shopify Tags']",
                                                "filter": {
                                                    "type": "list",
                                                    "operator": "contains",
                                                    "value": "active-membership"
                                                }
                                            }
                                        ]
                                    }
                                ]
                            }
                        },
                        "links": {
                            "next_if_true": None,
                            "next_if_false": "action_16"
                        }
                    },
                    # SMS #4
                    {
                        "temporary_id": "action_16",
                        "type": "send-sms",
                        "data": {
                            "message": {
                                "body": "Basin:\n\nLast day to grab your $29 unlimited 2-week climbing pass. Link expires tonight →\nhttps://mcsms.io/w63a4a\n",
                                "shorten_links": True,
                                "add_org_prefix": True,
                                "add_info_link": True,
                                "add_opt_out_language": True,
                                "smart_sending_enabled": True,
                                "sms_quiet_hours_enabled": True,
                                "name": "Text message #4"
                            },
                            "status": "draft"
                        },
                        "links": {
                            "next": "action_17"
                        }
                    },
                    # 1 hour delay
                    {
                        "temporary_id": "action_17",
                        "type": "time-delay",
                        "data": {
                            "unit": "hours",
                            "value": 1,
                            "secondary_value": 0,
                            "timezone": "profile"
                        },
                        "links": {
                            "next": "action_18"
                        }
                    },
                    # Email #5 (final)
                    {
                        "temporary_id": "action_18",
                        "type": "send-email",
                        "data": {
                            "message": {
                                "from_email": "info@basinclimbing.com",
                                "from_label": "Basin Climbing and Fitness",
                                "reply_to_email": "info@basinclimbing.com",
                                "cc_email": None,
                                "bcc_email": None,
                                "subject_line": "Last chance! $29 climbing pass expires tonight",
                                "preview_text": "If you haven't already- take advantage of this offer",
                                "template_id": "XXS55i",
                                "smart_sending_enabled": True,
                                "name": "Email #5"
                            },
                            "status": "draft"
                        },
                        "links": {
                            "next": None
                        }
                    }
                ],
                "entry_action_id": "action_1",
                "reentry_criteria": {
                    "duration": 1,
                    "unit": "alltime"
                }
            }
        }
    }
}

print("Creating TEMP Retry Flow (Skip Email 1)...")
print(f"List ID: {LIST_ID}")

url = "https://a.klaviyo.com/api/flows"
response = requests.post(url, headers=headers, json=flow_definition, timeout=60)

if response.status_code in [200, 201]:
    result = response.json()
    flow_id = result['data']['id']
    flow_name = result['data']['attributes']['name']
    print(f"\n✅ Flow created successfully!")
    print(f"   ID: {flow_id}")
    print(f"   Name: {flow_name}")
    print(f"   Status: draft")
    print(f"\n📋 NEXT STEPS:")
    print(f"   1. Go to Klaviyo and review the flow")
    print(f"   2. Turn it ON (set to Live)")
    print(f"   3. Run: python -m data_pipeline.klaviyo_temp_retry_flow --add-members")
else:
    print(f"\n❌ Error creating flow: {response.status_code}")
    print(response.text)
