-- 主表语句
CREATE TABLE "zl"."zl_log_info_main" (
"id" int8 DEFAULT nextval('seq_zl_log_info_main_id'::regclass) NOT NULL,
"server_id" int4,
"cont" jsonb,
"time" timestamp(6),
"file" varchar(150) COLLATE "default",
"time_log" int8 DEFAULT 0,
"uuid" int8 DEFAULT 0,
PRIMARY KEY ("id")
) 
WITH (OIDS=FALSE) TABLESPACE game_log_space;
ALTER TABLE "zl"."zl_log_info_main" OWNER TO "game";
CREATE INDEX "zl_log_info_main_file_index" ON "zl"."zl_log_info_main" USING btree ("file" "pg_catalog"."text_ops");
CREATE INDEX "zl_log_info_main_server_index" ON "zl"."zl_log_info_main" USING btree ("server_id" "pg_catalog"."int4_ops");
CREATE INDEX "zl_log_info_main_time_index" ON "zl"."zl_log_info_main" USING btree ("time" "pg_catalog"."timestamp_ops");
ALTER TABLE "zl"."zl_log_info_main" CLUSTER ON "zl_log_info_main_time_index";
CREATE INDEX "zl_log_info_main_time_log_index" ON "zl"."zl_log_info_main" USING btree ("time_log" "pg_catalog"."int8_ops");
CREATE INDEX "zl_log_info_main_uuid_index" ON "zl"."zl_log_info_main" USING btree ("uuid" "pg_catalog"."int8_ops");

-- 分表函数

CREATE OR replace FUNCTION zl.fn_create_table_log2 (yearx int) RETURNS integer
 AS $$  
declare aa integer;
declare ret integer;
declare table_ text;
BEGIN    
   FOR aa IN 1..12 LOOP     
    table_:='zl_log_info'||yearx||'_'||aa;
     -- raise notice '%',table_; 
			-- 建表语句开始
			EXECUTE format('CREATE TABLE if not exists "%1$s" ("id" int8 DEFAULT nextval(''zl.seq_zl_log_info_main_id''::regclass) NOT NULL,"server_id" int4,"cont" jsonb,"time" timestamp(6),"file" varchar(150) COLLATE "default","time_log" int8 DEFAULT 0,	"uuid" int8 DEFAULT 0,PRIMARY KEY ("id"))	INHERITS ("zl_log_info_main")WITH (OIDS=FALSE) TABLESPACE game_log_space',table_);	
      EXECUTE format('ALTER TABLE "%1$s" OWNER TO "game"',table_); 
      EXECUTE format('CREATE INDEX "%1$s_file_index" ON "%1$s" USING btree ("file" "pg_catalog"."text_ops")',table_);
      EXECUTE format('CREATE INDEX "%1$s_server_index" ON "%1$s" USING btree ("server_id" "pg_catalog"."int4_ops")',table_); 
      EXECUTE format('CREATE INDEX "%1$s_time_index" ON "%1$s" USING btree ("time" "pg_catalog"."timestamp_ops")',table_); 
      EXECUTE format('CREATE INDEX "%1$s_time_log_index" ON "%1$s" USING btree ("time_log" "pg_catalog"."int8_ops")',table_); 
      EXECUTE format('CREATE INDEX "%1$s_uuid_index" ON "%1$s" USING btree ("uuid" "pg_catalog"."int8_ops")',table_); 

			-- 建表语句结速
    ret:=ret+1;
   END LOOP;
RETURN ret;   
END; 
$$ LANGUAGE plpgsql; 
ALTER FUNCTION zl.fn_create_table_log2(yearx int)
    OWNER TO game;
COMMENT ON FUNCTION zl.fn_create_table_log2(yearx int)
    IS '建一年数据分表语句';
    
    -- 测试drop
drop table zl.zl_log_info2018_1;
drop table zl.zl_log_info2018_2;
drop table zl.zl_log_info2018_3;
drop table zl.zl_log_info2018_4;
drop table zl.zl_log_info2018_5;
drop table zl.zl_log_info2018_6;
drop table zl.zl_log_info2018_7;
drop table zl.zl_log_info2018_8;
drop table zl.zl_log_info2018_9;
drop table zl.zl_log_info2018_10;
drop table zl.zl_log_info2018_11;
drop table zl.zl_log_info2018_12;
