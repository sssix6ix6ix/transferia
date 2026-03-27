-- parent can has any numbers, but in test parent directly has only 1-digit numbers.
CREATE TABLE parent (
    id   INT  NOT NULL PRIMARY KEY,
    name TEXT NOT NULL
);

-- child_1 have 2-digits numbers.
CREATE TABLE child_1 (
    CHECK (id >= 10 AND id < 100)
) INHERITS (parent);
ALTER TABLE child_1 ADD PRIMARY KEY (id);

-- child_2 have 3-digits numbers.
CREATE TABLE child_2 (
    CHECK (id >= 100 AND id < 1000)
) INHERITS (parent);
ALTER TABLE child_2 ADD PRIMARY KEY (id);

-- 2 rows directly in parent, not routed to any child.
INSERT INTO parent VALUES (1, 'parent_1'), (2, 'parent_2');

-- 3 rows in child_1.
INSERT INTO child_1 VALUES (10, 'child_1_1'), (11, 'child_1_2'), (99, 'child_1_3');

-- 4 rows in child_2.
INSERT INTO child_2 VALUES (100, 'child_2_1'), (101, 'child_2_2'), (102, 'child_2_3'), (999, 'child_2_4');
