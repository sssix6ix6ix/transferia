CREATE SCHEMA certificate;

CREATE TABLE certificate.certificate_template (
    certificate_event_id        UUID                        NOT NULL,
    certificate_type_id         UUID                        NOT NULL,
    file_content_type           VARCHAR(255)                NOT NULL,
    file_data                   BYTEA                       NOT NULL,
    polls_link                  TEXT,
    need_email_sending          BOOLEAN                     NOT NULL,
    created_at                  TIMESTAMP WITH TIME ZONE    NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMP WITH TIME ZONE    NOT NULL DEFAULT NOW(),
    created_by_id               UUID                        NOT NULL,
    updated_by_id               UUID                        NOT NULL,
    created_by_user_name        VARCHAR(255),
    updated_by_user_name        VARCHAR(255),
    processed_file_content_type VARCHAR(255)                NOT NULL DEFAULT '',
    processed_file_data         BYTEA                       NOT NULL DEFAULT '\x'::BYTEA,
    is_pre_made                 BOOLEAN                     NOT NULL DEFAULT FALSE,
    is_additional_data_visible  BOOLEAN                     NOT NULL DEFAULT FALSE,
    is_polls_required           BOOLEAN                     NOT NULL DEFAULT TRUE,

    CONSTRAINT PK_certificate_template PRIMARY KEY (certificate_event_id, certificate_type_id)
);

CREATE INDEX IX_certificate_template_certificate_type_id
    ON certificate.certificate_template (certificate_type_id);
