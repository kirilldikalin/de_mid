INSERT INTO mart.jira_issue_current
SELECT
    issue_key,
    max(updated_at) AS updated_at,
    anyLast(status) AS status,
    anyLast(assignee) AS assignee,
    sum(spent_hours) AS spent_hours
FROM raw.jira_issue_snapshots
WHERE project = 'DEPCONUX'
GROUP BY issue_key;