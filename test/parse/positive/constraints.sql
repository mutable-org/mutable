CREATE TABLE test (
    id INT(4) PRIMARY KEY,
    not_null INT(4) NOT NULL,
    unique INT(4) UNIQUE,
    checked INT(4) CHECK ((checked < 42)),
    ref_id INT(4) REFERENCES ref(id),
    many_checks INT(4) CHECK ((many_checks > 13)) CHECK ((many_checks != 5)) CHECK (((many_checks < 1337) AND (many_checks > 808))),
    combined INT(4) UNIQUE CHECK ((combined != 13)) REFERENCES ref(combined) NOT NULL UNIQUE NOT NULL
);
