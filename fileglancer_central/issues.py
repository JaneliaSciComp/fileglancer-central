
from atlassian import Jira
import os

def create_jira_ticket(summary, description, project_key="DEFAULT", issue_type="Task"):
    """
    Creates a new JIRA ticket using the Atlassian Python API
    
    Args:
        summary (str): Title/summary of the ticket
        description (str): Detailed description of the ticket
        project_key (str): The project key where ticket should be created (e.g. 'PROJ')
        issue_type (str): Type of issue (e.g. 'Task', 'Bug', 'Story')
        
    Returns:
        str: The key of the created issue (e.g. 'PROJ-123')
    """
    
    # Get JIRA credentials from environment variables
    jira_server = "https://issues.hhmi.org/issues"
    jira_token = os.getenv("JIRA_TOKEN")
    
    if not all([jira_server, jira_token]):
        raise ValueError("Missing required JIRA credentials in environment variables")
    
    try:
        # Initialize JIRA client
        print(jira_server, jira_token)
        jira = Jira(
            url=jira_server,
            token=jira_token
        )
        
        # Prepare issue fields
        issue_dict = {
            'project': {'key': project_key},
            'summary': summary,
            'description': description,
            'issuetype': {'name': issue_type},
        }
        
        # Create the issue
        new_issue = jira.issue_create(fields=issue_dict)
        return new_issue['key']
        
    except Exception as e:
        import traceback
        print("Full stack trace:")
        traceback.print_exc()
        raise Exception(f"Failed to create JIRA ticket: {str(e)}")

if __name__ == "__main__":
    # Example usage
    try:
        ticket_key = create_jira_ticket(
            summary="Test Ticket",
            description="This is a test ticket created via API *bold*\nnew line",
            project_key="JW",
            issue_type="Bug"
        )
        print(f"Successfully created ticket: {ticket_key}")
    except Exception as e:
        print(f"Error: {str(e)}")

