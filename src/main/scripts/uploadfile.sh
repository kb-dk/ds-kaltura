#!/bin/bash

SCRIPT_DIR=$(dirname "$(readlink -f -- ${BASH_SOURCE[0]})")

check_file() {
    local F="$1"
    if [[ -s "$F" || -d "$F" ]]; then
        return
    fi
    
    >&2 echo "Error: Unable to locate $F"
    echo ""
    echo "Probable cause: The script is running from the code checkout instead of the end delivery."
    echo "                To test the Main method during development, use the MainTest class."
    exit 2
}

check_file "$SCRIPT_DIR/../conf/appEnv.sh"
check_file "$SCRIPT_DIR/../lib/"

source "$SCRIPT_DIR/../conf/appEnv.sh"

MAIN_CLASS=dk.kb.kaltura.jobs.UploadFile

if [ -z "$APP_CONFIG" ]; then
    echo "APP_CONFIG has not been set" 1>&2
    exit 1
fi

CLASS_PATH="${CLASS_PATH_OVERRIDE:-"$SCRIPT_DIR/../lib/*"}"
JAVA_OPTS=${JAVA_OPTS:-"-Xmx256m -Xms256m"}
LOG_EMAIL=${LOG_EMAIL:-"nobody@example.com"} # Set to a real email in appEnv.sh to enable
LOGBACK_CONF=${LOGBACK_CONF:-""$SCRIPT_DIR/../conf/ds-kaltura-logback.xml""}

START_TIME=$(date +"%Y-%m-%d %H:%M")
java $JAVA_OPTS -classpath "$CLASS_PATH" -Dlogback.configurationFile="$LOGBACK_CONF" -Ddk.kb.applicationConfig="$SCRIPT_DIR/../conf/$APP_CONFIG" "$MAIN_CLASS" "$@"
EXIT_CODE=$?
END_TIME=$(date +"%Y-%m-%d %H:%M")


# Optional emailing of logfile below
if [[ "$LOG_EMAIL" != "nobody@example.com" ]]; then
  LOG_FILE=${LOG_FILE:-"$(grep 'name="LOGFILE"' "$LOGBACK_CONF" | sed -e 's%.*value="\([^"]*\)".*%\1%' -e "s%[$]{user.home}%$HOME%")"}
  MESSAGE_BODY=$(cat <<EOF
Job: ds-kaltura
Started: $START_TIME
Ended: $END_TIME
Exit code: $EXIT_CODE
EOF
)
  echo "$MESSAGE_BODY" | mail -s "ds-kaltura log $START_TIME" $LOG_EMAIL -A "$LOG_FILE"
fi
