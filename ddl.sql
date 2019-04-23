DROP TABLE events IF EXISTS;
DROP TABLE visits IF EXISTS;

CREATE TABLE events (
	receiver_id integer not null,
	customer_id integer not null,
	event_type smallint not null,
	event_time timestamp not null,
	primary key (receiver_id, event_time)
);
PARTITION TABLE events ON COLUMN receiver_id;
CREATE INDEX idx_events_receiver_customer ON events(receiver_id, customer_id);

CREATE TABLE visits (
	receiver_id integer not null,
	customer_id integer not null,
	location_id integer not null,
	visit_time timestamp not null,
	primary key (receiver_id, visit_time)
);
PARTITION TABLE visits ON COLUMN receiver_id;

create procedure newEvent partition on table events column receiver_id as insert into events values (?, ?, ?, NOW());
create procedure newVisit partition on table visits column receiver_id as insert into visits values (?, ?, ?, NOW());

CREATE VIEW events_by_customer AS SELECT customer_id, event_type, COUNT(*) FROM events GROUP BY customer_id, event_type;
CREATE VIEW busiest_customer AS SELECT customer_id, COUNT(*) FROM visits GROUP BY customer_id;
