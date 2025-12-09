# Omni Semantic Views - Terraform & GitHub Actions

This repository contains Omni topic and view definitions, with automated Terraform generation and deployment to Snowflake semantic views via GitHub Actions.

## Overview

When you merge a PR that modifies Omni topic or view YAML files, the GitHub Actions workflow will:

1. **Detect changed files** and find which topics are affected
2. **Generate Terraform resources** only for affected topics (not all topics)
3. **Plan the changes** to see what will be created/updated in Snowflake
4. **Apply the changes** automatically (if PR is merged to main)
5. **Commit the generated Terraform files** back to the repository

### Smart Change Detection

The workflow intelligently detects what needs to be regenerated:

- **View file changed** → Finds all topics that use that view → Generates only those topics
- **Topic file changed** → Generates only that specific topic
- **relationships.yaml changed** → Generates all topics (since relationships affect all joins)
- **Multiple files changed** → Generates union of all affected topics

This means faster runs and only the necessary semantic views are updated!

## Repository Structure

```
.
├── omni/                    # Omni topic and view YAML files
│   ├── Ecomm Demo/
│   ├── Snowflake/
│   └── relationships.yaml
├── terraform/               # Terraform configuration and generated resources
│   ├── provider.tf
│   ├── versions.tf
│   ├── terraform.tfvars.example
│   └── *.tf                 # Generated semantic view resources
├── generate_semantic_views.py  # Script to generate Terraform from Omni YAML
├── requirements.txt         # Python dependencies
└── .github/workflows/       # GitHub Actions workflows
```

## Setup

### 1. Configure GitHub Secrets

**Recommended: Use Environment Secrets** (better security and flexibility)

1. Go to your repository Settings → Environments
2. Click "New environment" and create an environment named `production`
3. In the environment, go to "Secrets and variables" → "Actions"
4. Add the following secrets:
   - `SNOWFLAKE_ACCOUNT_NAME` - Your Snowflake account identifier
   - `SNOWFLAKE_ORGANIZATION_NAME` - Your Snowflake organization (if applicable)
   - `SNOWFLAKE_USER` - Snowflake username
   - `SNOWFLAKE_OAUTH_ACCESS_TOKEN` - Personal Access Token (PAT) from Snowflake
   - `SNOWFLAKE_WAREHOUSE` - Snowflake warehouse name
   - `SNOWFLAKE_DATABASE` - Database where semantic views will be created
   - `SNOWFLAKE_ROLE` - Snowflake role to use

**Alternative: Repository Secrets** (simpler, but less flexible)

If you prefer repository secrets instead:
1. Go to Settings → Secrets and variables → Actions → Repository secrets
2. Add the same secrets listed above
3. Remove the `environment: production` line from the workflow file

**Why Environment Secrets?**
- ✅ Better security: Can require approval before deployment
- ✅ Flexibility: Can have different secrets for dev/staging/prod
- ✅ Audit trail: Better tracking of who deployed what
- ✅ Protection rules: Can restrict which branches can deploy

**To get a Personal Access Token:**
1. Log into Snowflake Web UI
2. Go to User Profile → Personal Access Tokens
3. Generate a new token
4. Copy the token and add it as a GitHub secret

### 2. Local Development (Optional)

If you want to test locally:

```bash
# Install Python dependencies
pip install -r requirements.txt

# Copy and configure Terraform variables
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
# Edit terraform/terraform.tfvars with your Snowflake credentials

# Generate Terraform for all topics
python generate_semantic_views.py --output-dir terraform --project-root omni

# Or generate for a specific topic
python generate_semantic_views.py order_items --output-dir terraform --project-root omni

# Review and apply
cd terraform
terraform init
terraform plan
terraform apply
```

## Workflow

### Automatic Generation on PR Merge

1. **Make changes** to Omni YAML files in `omni/`
   - Example: Edit `omni/Ecomm Demo/ECOMM/order_items.view.yaml`
2. **Open a PR** with your changes
3. **Workflow runs automatically** (on PR open/update):
   - Detects that `order_items.view.yaml` changed
   - Finds topics that use `ecomm__order_items` (e.g., `order_items` topic)
   - Generates Terraform only for affected topics
   - Shows preview in PR comments
4. **Review and approve** the PR
5. **Merge to main** - The workflow will automatically:
   - Generate Terraform resources for affected topics
   - Run `terraform plan` to show what will change
   - Run `terraform apply` to deploy to Snowflake
   - Commit the generated `.tf` files back to the repo

### Manual Trigger

You can also manually trigger the workflow:

1. Go to Actions tab in GitHub
2. Select "Generate Semantic Views" workflow
3. Click "Run workflow"
4. Optionally specify a topic name to generate only that topic

## Generated Semantic Views

Semantic views are named using the pattern: `omni_<topic_name>_sv`

For example:
- Topic `order_items.topic.yaml` → Semantic view `omni_order_items_sv`
- Topic `opportunity.topic.yaml` → Semantic view `omni_opportunity_sv`

## Testing Change Detection Locally

You can test the change detection script locally:

```bash
# Test with git diff
python .github/scripts/find_affected_topics.py \
  --base-ref main \
  --head-ref feature-branch \
  --project-root omni

# Test with specific files
python .github/scripts/find_affected_topics.py \
  --changed-files \
    omni/Ecomm\ Demo/ECOMM/order_items.view.yaml \
    omni/Ecomm\ Demo/Orders\ \&\ Fulfillment/order_items.topic.yaml \
  --project-root omni
```

## Troubleshooting

### Workflow doesn't trigger

- Ensure the PR is merged to `main` or `master` branch
- Check that files were modified in `omni/**/*.topic.yaml` or `*.view.yaml`
- Verify the workflow file exists at `.github/workflows/generate-semantic-views.yml`

### No affected topics found

- Check that the changed view files are actually referenced by topics
- Verify view names match between files and topic references
- Check the workflow logs for the "Find affected topics" step output
- View files should have a comment like `# Reference this view as ecomm__order_items`

### Terraform apply fails

- Check GitHub Actions logs for detailed error messages
- Verify all GitHub secrets are set correctly
- Ensure the Snowflake user has permissions to create semantic views
- Check that the database and schema exist in Snowflake

### No changes detected

- The workflow only generates Terraform if there are actual changes
- If you see "No changes detected", the semantic views are already up to date

## Files

- `generate_semantic_views.py` - Python script that converts Omni YAML to Terraform
- `.github/scripts/find_affected_topics.py` - Script that detects changed files and finds affected topics
- `terraform/provider.tf` - Terraform provider configuration
- `terraform/versions.tf` - Terraform version requirements
- `.github/workflows/generate-semantic-views.yml` - GitHub Actions workflow

## Support

For issues or questions:
1. Check the GitHub Actions logs for error details
2. Review the generated Terraform files in `terraform/`
3. Verify your Snowflake credentials and permissions

