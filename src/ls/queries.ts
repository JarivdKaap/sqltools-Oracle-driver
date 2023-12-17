import { IBaseQueries, ContextValue, NSDatabase} from '@sqltools/types';
import queryFactory from '@sqltools/base-driver/dist/lib/factory';

function escapeTableName(table: Partial<NSDatabase.ITable>) {
  return `${table.label || table.toString()}`;
}

const describeTable: IBaseQueries['describeTable'] = queryFactory`
SELECT * from user_tab_columns t
 WHERE t.TABLE_NAME = '${p => escapeTableName(p)}'
 ORDER BY t.COLUMN_NAME
`;

const fetchColumns: IBaseQueries['fetchColumns'] = queryFactory`
select t.COLUMN_NAME as "label",
       '${ContextValue.COLUMN}' as "type",
       t.TABLE_NAME as "table",
       t.DATA_TYPE as "dataType",
       t.DATA_LENGTH as "size",
       t.owner as "schema",
       SYS_CONTEXT ('USERENV', 'DB_NAME') as "database",
       t.DATA_DEFAULT as "defaultValue",
       t.owner as "catalog",
       t.NULLABLE    as "isNullable",
       upper(t.DATA_TYPE || 
         decode(t.DATA_LENGTH,null,'',
           '(' || t.DATA_LENGTH || decode(t.DATA_PRECISION,null,'', 
                                          ',' || t.DATA_PRECISION) || ')')) as "detail",
       0 as "isPk",
       0 as "isFk"
  from all_tab_columns t 
 where t.TABLE_NAME = '${p => escapeTableName(p)}'
 order by
   t.TABLE_NAME, 
   t.COLUMN_ID
`;

const fetchRecords: IBaseQueries['fetchRecords'] = queryFactory`
SELECT * FROM
  (SELECT ROWNUM NO,t.*
     FROM ${p => escapeTableName(p.table)} t)t
 WHERE t.NO > ${p => p.offset || 0}
   AND t.NO <= ${p => p.offset || 0} + ${p => p.limit || 50};
`;

const countRecords: IBaseQueries['countRecords'] = queryFactory`
SELECT count(1) AS "total"
FROM ${p => escapeTableName(p.table)};
`;


const fetchTables: IBaseQueries['fetchTables'] = queryFactory`
select t.table_name as "label",
       '${ContextValue.TABLE}' as "type",
       t.owner as "schema",
       SYS_CONTEXT ('USERENV', 'DB_NAME') as "database",
       t.owner as "catalog",
       0 as "isView"
  from all_tables t
 order by
 decode(t.owner,user,0,1),
 t.table_name
`;
const fetchViews: IBaseQueries['fetchTables'] = queryFactory`
select t.view_name as "label",
       '${ContextValue.VIEW}' as "type",
       t.owner as "schema",
       SYS_CONTEXT ('USERENV', 'DB_NAME') as "database",
       t.owner as "catalog",
       1 as "isView"
  from all_views t
 order by
 decode(t.owner,user,0,1),
 t.view_name
`;
const fetchIndexes: IBaseQueries['fetchTables'] = queryFactory`
select t.index_name as "label",
       'INDEX' as "type",
       SYS_CONTEXT ('USERENV', 'DB_NAME') as "database",
       0 as "isView",
       '${ContextValue.NO_CHILD}' as "childType",
       'symbol-object' as "iconId"
  from user_indexes t
 order by
 t.index_name
`;
const fetchPackages: IBaseQueries['fetchTables'] = queryFactory`
select t.object_name as "label",
       t.object_name as "object_name",
       'PACKAGE' as "type",
       SYS_CONTEXT ('USERENV', 'DB_NAME') as "database",
       0 as "isView",
       'symbol-package' as "iconId"
  from user_objects t
 where t.object_type = 'PACKAGE'
 order by
 t.object_name
`;
const fetchPackageDetails: IBaseQueries['fetchTables'] = queryFactory`
select t.object_name||' Body' as "label",
       'PACKAGE_BODY' as "type",
       SYS_CONTEXT ('USERENV', 'DB_NAME') as "database",
       0 as "isView",
       'ARGUMENTS' as "childType",
       0 as seq,
       'symbol-package' as "iconId"
  from user_objects t
 where t.object_type = 'PACKAGE BODY'
   and object_name = '${p => p.object_name}'
 union all
 select t.procedure_name as "label",
       'ARGUMENTS' as "type",
       SYS_CONTEXT ('USERENV', 'DB_NAME') as "database",
       0 as "isView",
       '${ContextValue.NO_CHILD}' as "childType",
       t.subprogram_id as seq,
       case when exists (
          select 1
            from user_arguments a
            where a.object_name = t.procedure_name
              and a.package_name = t.object_name
              and a.in_out = 'OUT'
         ) THEN 'symbol-method' ELSE 'symbol-function' END as "iconId"
  from user_procedures t
 where t.object_type = 'PACKAGE'
   and object_name = '${p => p.object_name}'
   and t.procedure_name is not null
 order by seq, "label"
`;
const fetchProcedures: IBaseQueries['fetchTables'] = queryFactory`
select t.object_name as "label",
       'PROCEDURE' as "type",
       SYS_CONTEXT ('USERENV', 'DB_NAME') as "database",
       0 as "isView",
       'symbol-method' as "iconId"
  from user_objects t
 where t.object_type = 'PROCEDURE'
 order by
 t.object_name
`;
const fetchProcedureDetails: IBaseQueries['fetchTables'] = queryFactory`
select argument_name as "label",
       'ARGUMENT' as "type",
       data_type as "dataType",
       default_value as "defaultValue",
       '${ContextValue.NO_CHILD}' as "childType",
       'symbol-module' as "iconId"
  from user_arguments
 where object_name = 'CONCATENATE'
   and argument_name is not null
 order by sequence
`;
const fetchFunctions: IBaseQueries['fetchTables'] = queryFactory`
select t.object_name as "label",
       '${ContextValue.FUNCTION}' as "type",
       SYS_CONTEXT ('USERENV', 'DB_NAME') as "database",
       0 as "isView",
       'symbol-function' as "iconId"
  from user_objects t
 where t.object_type = 'FUNCTION'
 order by
 t.object_name
`;

const searchTables: IBaseQueries['searchTables'] = queryFactory`
select  t.label        "label",
        t.type         "type",
        t.isView       "isView",
        t.description  "description",
        t.schema       "schema",
        t.database     "database",
        t.catalog      "catalog",
        t.detail       "detail"
from( select t.*
       from(
            select  t.table_name as                       label,
                    '${ContextValue.TABLE}' as            type,
                    0 as                                  isView,
                    'table' as                            description,
                    t.owner as                            schema,
                    SYS_CONTEXT ('USERENV', 'DB_NAME') as database,
                    t.owner as                               catalog,
                    t.owner || '.' || t.table_name as     detail
              from all_tables t
              where t.owner = user
            union all
            select  t.view_name as                        label,
                    '${ContextValue.VIEW}' as             type,
                    1 as                                  isView,
                    'view' as                             description,
                    t.owner as                            schema,
                    SYS_CONTEXT ('USERENV', 'DB_NAME') as database,
                    t.owner as catalog,
                    t.owner || '.' || t.view_name as      detail
              from all_views t
              where t.owner = user
            union all
            select  t.table_name as                       label,
                    '${ContextValue.TABLE}' as            type,
                    0 as                                  isView,
                    'table' as                            description,
                    t.owner as                            schema,
                    SYS_CONTEXT ('USERENV', 'DB_NAME') as database,
                    t.owner as                               catalog,
                    t.owner || '.' || t.table_name as     detail
              from all_tables t
              where t.owner <> user
            union all
            select  t.view_name as                        label,
                    '${ContextValue.VIEW}' as             type,
                    1 as                                  isView,
                    'view' as                             description,
                    t.owner as                            schema,
                    SYS_CONTEXT ('USERENV', 'DB_NAME') as database,
                    t.owner as catalog,
                    t.owner || '.' || t.view_name as      detail
              from all_views t
              where t.owner <> user
            ) t
    where 1 = 1
    ${p => p.search ? `AND (
      lower(t.label) LIKE '${p.search.toLowerCase()}%'
      OR lower(t.schema || '.' || t.label) LIKE '${p.search.toLowerCase()}%'
    )` : ''}
  )t
  where rownum <= ${p => p.limit || 1500}

`;
const searchColumns: IBaseQueries['searchColumns'] = queryFactory`
select t.*
  from(select t.COLUMN_NAME as "label",
              '${ContextValue.COLUMN}' as "type",
              t.TABLE_NAME as "table",
              t.DATA_TYPE as "dataType",
              t.DATA_LENGTH as "size",
              t.owner as "schema",
              SYS_CONTEXT ('USERENV', 'DB_NAME') as "database",
              t.owner as "catalog",
              t.DATA_DEFAULT as "defaultValue",
              t.NULLABLE    as "isNullable",
              upper(t.DATA_TYPE || 
              decode(t.DATA_LENGTH,null,'',
                    '(' || t.DATA_LENGTH || decode(t.DATA_PRECISION,null,'', 
                                                  ',' || t.DATA_PRECISION) || ')')) as "detail",
              0 as "isPk",
              0 as "isFk"
         from all_tab_columns t
        where 1 = 1 
          ${p => p.tables.filter(t => !!t.label).length
          ? `AND LOWER(t.TABLE_NAME) IN (${p.tables.filter(t => !!t.label).map(t => `'${t.label}'`.toLowerCase()).join(', ')})`
          : ''
          }
         ${p => p.search
          ? `AND (
          lower(t.TABLE_NAME || '.' || t.COLUMN_NAME) LIKE '${p.search.toLowerCase()}%'
          OR lower(t.owner || '.' || t.TABLE_NAME) LIKE '${p.search.toLowerCase()}%'
          OR lower(t.COLUMN_NAME) LIKE '${p.search.toLowerCase()}%'
          )`
          : ''
          }
        order by
        decode(t.owner,user,0,1),
        t.TABLE_NAME, 
        t.COLUMN_ID)t
where rownum <= ${p => p.limit || 1500}
`;

export default {
  describeTable,
  countRecords,
  fetchColumns,
  fetchRecords,
  fetchTables,
  fetchViews,
  fetchIndexes,
  fetchPackages,
  fetchPackageDetails,
  fetchProcedures,
  fetchFunctions,
  fetchProcedureDetails,
  searchTables,
  searchColumns
}
