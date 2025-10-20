CREATE DATABASE houses_rental
    WITH ENCODING 'UTF8'
    LC_COLLATE='es_CO.UTF-8'
    LC_CTYPE='es_CO.UTF-8'
    TEMPLATE=template0
    OWNER=postgres;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TYPE "snapshot_status" AS ENUM (
  'active',
  'inactive',
  'removed'
);

CREATE TABLE "listings" (
  "id" uuid PRIMARY KEY DEFAULT (uuid_generate_v4()),
  "provider_listing_id" varchar(200) UNIQUE,
  "title" text,
  "property_type" varchar(50),
  "city" varchar(100),
  "neighborhood" varchar(200),
  "price" numeric,
  "currency" varchar(10) DEFAULT 'COP',
  "rooms" int,
  "bathrooms" int,
  "parkings" int,
  "area_m2" numeric,
  "link" text,
  "image_url" text,
  "features" jsonb,
  "metadata" jsonb,
  "first_seen" timestamptz DEFAULT (now()),
  "last_seen" timestamptz DEFAULT (now()),
  "active" boolean DEFAULT true
);

CREATE TABLE "listing_snapshots" (
  "id" serial PRIMARY KEY,
  "listing_id" uuid NOT NULL,
  "provider_listing_id" varchar(200),
  "scraped_at" timestamptz DEFAULT (now()),
  "price" numeric,
  "currency" varchar(10) DEFAULT 'COP',
  "status" snapshot_status DEFAULT 'active',
  "raw_json" jsonb
);

CREATE UNIQUE INDEX ON "listings" ("provider_listing_id");

CREATE INDEX ON "listings" ("city");

CREATE INDEX ON "listings" ("neighborhood");

CREATE INDEX ON "listings" ("active");

CREATE INDEX ON "listing_snapshots" ("listing_id", "scraped_at");

CREATE INDEX ON "listing_snapshots" ("provider_listing_id", "scraped_at");

COMMENT ON COLUMN "listings"."id" IS 'Internal canonical listing ID';

COMMENT ON COLUMN "listings"."provider_listing_id" IS 'Espacio Urbano listing ID (used for upserts)';

COMMENT ON COLUMN "listings"."title" IS 'Property title or headline';

COMMENT ON COLUMN "listings"."property_type" IS 'Apartment, house, studio, etc.';

COMMENT ON COLUMN "listings"."price" IS 'Last observed price (COP)';

COMMENT ON COLUMN "listings"."link" IS 'Direct URL to the listing';

COMMENT ON COLUMN "listings"."features" IS 'Free-form attributes (e.g., furnished, parking, pets allowed)';

COMMENT ON COLUMN "listings"."metadata" IS 'Extra scraped metadata';

COMMENT ON COLUMN "listings"."first_seen" IS 'When this listing was first scraped';

COMMENT ON COLUMN "listings"."last_seen" IS 'When this listing was last observed';

COMMENT ON COLUMN "listings"."active" IS 'Whether listing still appears active';

COMMENT ON COLUMN "listing_snapshots"."id" IS 'Each row is a single immutable scrape event';

COMMENT ON COLUMN "listing_snapshots"."provider_listing_id" IS 'Copied for convenience/reference';

COMMENT ON COLUMN "listing_snapshots"."scraped_at" IS 'Timestamp of the scrape';

COMMENT ON COLUMN "listing_snapshots"."price" IS 'Price observed at this point in time';

COMMENT ON COLUMN "listing_snapshots"."raw_json" IS 'Complete raw payload or parsed structure';

ALTER TABLE "listing_snapshots" ADD FOREIGN KEY ("listing_id") REFERENCES "listings" ("id");