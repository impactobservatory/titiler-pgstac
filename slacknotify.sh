#!/usr/bin/env bash
set -euo pipefail

SLACK_WEBHOOK=${SLACK_WEBHOOK}
ENVIRONMENT="${CI_ENVIRONMENT_NAME:=common_environment}"

function print_slack_summary() {

    local slack_msg_header
    local slack_msg_body

    # Populate header and define slack channels
    slack_msg_header=":x: Deploy of ${CI_PROJECT_TITLE} to ${ENVIRONMENT} failed"
    if [[ "${CI_JOB_STATUS}" == "success" ]]; then
        slack_msg_header=":white_check_mark: Deploy of ${CI_PROJECT_TITLE} to ${ENVIRONMENT} succeeded"
    fi

    # Populate slack message body
    slack_msg_body="${CI_JOB_NAME} with job id <${CI_PROJECT_URL}/-/jobs/${CI_JOB_ID}|${CI_JOB_ID}> by ${GITLAB_USER_NAME} \n<${CI_PROJECT_URL}/-/commit/$(git rev-parse HEAD)|$(git rev-parse --short HEAD)> - ${CI_COMMIT_REF_NAME} "
    
    cat <<-SLACK
            {
                "blocks": [
                  {
                          "type": "section",
                          "text": {
                                  "type": "mrkdwn",
                                  "text": "${slack_msg_header}"
                          }
                  },
                  {
                          "type": "divider"
                  },
                  {
                          "type": "section",
		                  "text": {
                                  "type": "mrkdwn",
                                  "text": "${slack_msg_body}"
                          }
                  }
                ]
}
SLACK
}

function share_slack_update() {
    
    curl -X POST                                           \
        --data-urlencode "payload=$(print_slack_summary)"  \
        "${SLACK_WEBHOOK}"

}

share_slack_update