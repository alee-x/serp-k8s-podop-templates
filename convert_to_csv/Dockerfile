FROM python:3.9

# Reset to root to run installation tasks
USER 0

COPY requirements.txt .
RUN pip3 install -r requirements.txt && mkdir pyfiles

# Specify the User that the actual main process will run as
ARG app_uid=1001
USER ${app_uid}

COPY src pyfiles/.