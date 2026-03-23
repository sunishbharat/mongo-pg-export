from pymongo import MongoClient
import pandas as pd
import json

client = MongoClient("mongodb://localhost:27017/")
db = client["JiraReposAnon"]

collections = db.list_collection_names()
print(f"Collections: {collections}")

# ── Extract 1000 issues with the most issue links across all collections ─
pipeline = [
    {"$match": {
        "fields.issuelinks": {"$exists": True, "$not": {"$size": 0}}
    }},
    {"$addFields": {
        "link_count": {"$size": {"$ifNull": ["$fields.issuelinks", []]}}
    }},
    {"$match": {"link_count": {"$gte": 3}}},
    {"$sort": {"link_count": -1}},
    {"$limit": 1000}
]

print("Querying MongoDB across all collections...")
raw_issues = []
for col_name in collections:
    results = list(db[col_name].aggregate(pipeline, allowDiskUse=True))
    print(f"  {col_name}: {len(results)} issues")
    raw_issues.extend(results)

# Re-sort and limit to top 1000 across all collections
raw_issues.sort(key=lambda x: x.get("link_count", 0), reverse=True)
raw_issues = raw_issues[:1000]
print(f"Found {len(raw_issues)} issues with 3+ links (top across all collections)")

# ── Flatten issues into flat rows ──────────────────────────────────────
def flatten_issue(doc):
    f = doc.get("fields", {})
    return {
        "key":          doc.get("key", ""),
        "summary":      f.get("summary", ""),
        "type":         (f.get("issuetype") or {}).get("name", ""),
        "status":       (f.get("status") or {}).get("name", ""),
        "priority":     (f.get("priority") or {}).get("name", ""),
        "project":      (f.get("project") or {}).get("key", ""),
        "project_name": (f.get("project") or {}).get("name", ""),
        "assignee":     (f.get("assignee") or {}).get("displayName", "unassigned"),
        "reporter":     (f.get("reporter") or {}).get("displayName", "unknown"),
        "created":      str(f.get("created", "")),
        "updated":      str(f.get("updated", "")),
        "resolved":     str(f.get("resolutiondate", "")),
        "story_points": f.get("story_points", f.get("customfield_10016")) or 0,
        "link_count":   doc.get("link_count", 0),
        "description":  (f.get("description") or "")[:300]
    }

issues_df = pd.DataFrame([flatten_issue(d) for d in raw_issues])
print(f"\nIssues shape: {issues_df.shape}")
print(issues_df["type"].value_counts())
print(issues_df["status"].value_counts())

# ── Flatten links into separate rows ───────────────────────────────────
links = []
for doc in raw_issues:
    from_key = doc.get("key", "")
    for link in (doc.get("fields") or {}).get("issuelinks", []):
        link_type_obj = link.get("type", {})
        
        if "outwardIssue" in link:
            links.append({
                "from_key":  from_key,
                "to_key":    link["outwardIssue"].get("key", ""),
                "link_type": link_type_obj.get("outward", "relates to"),
                "direction": "outward"
            })
        if "inwardIssue" in link:
            links.append({
                "from_key":  from_key,
                "to_key":    link["inwardIssue"].get("key", ""),
                "link_type": link_type_obj.get("inward", "relates to"),
                "direction": "inward"
            })

links_df = pd.DataFrame(links).drop_duplicates()
print(f"\nLinks shape: {links_df.shape}")
print(links_df["link_type"].value_counts().head(10))

# ── Save to CSV ────────────────────────────────────────────────────────
issues_df.to_csv("jira_issues_poc.csv", index=False)
links_df.to_csv("jira_links_poc.csv", index=False)

print("\nSaved:")
print("  jira_issues_poc.csv")
print("  jira_links_poc.csv")