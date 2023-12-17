import OracleDBLib from 'oracledb'
import AbstractDriver from '@sqltools/base-driver';
import queries from './queries';
import { IConnectionDriver, MConnectionExplorer, NSDatabase, ContextValue, Arg0 } from '@sqltools/types';
import parse from './parser';
import { v4 as generateId } from 'uuid';
import {Oracle_Diagnosis_Path} from '../constants';
import fs from 'fs';
import {performance} from 'perf_hooks'

const toBool = (v: any) => v && (v.toString() === '1' || v.toString().toLowerCase() === 'true' || v.toString().toLowerCase() === 'yes');

export interface PoolConfig{
  // 
  autoCommit?: boolean;
  lowerCase?: boolean; //lowcase for completion
  macroFile?: string; //file configured for macro substitution
  thickMode?: boolean;
  limitPrefetchRows?: boolean;
  privilege?: string;
  pool?: boolean;
}


export default class OracleDriver extends AbstractDriver<OracleDBLib.Pool, PoolConfig> implements IConnectionDriver {

  /**
   * If you driver depends on node packages, list it below on `deps` prop.
   * It will be installed automatically on first use of your driver.
   */
  public readonly deps: typeof AbstractDriver.prototype['deps'] = [{
    type: AbstractDriver.CONSTANTS.DEPENDENCY_PACKAGE,
    name: 'oracledb',
    version: '6.0.1',
  }];


  queries = queries;
  autoCommit = false;
  lowerCase = false;
  macroFile = '';
  maxRows = 0;
  privilege = 'Normal';
  privilegeMap = {'SYSDBA':this.lib.SYSDBA,'SYSOPER':this.lib.SYSOPER,'SYSASM':this.lib.SYSASM,'SYSBACKUP':this.lib.SYSBACKUP,
                    'SYSDG':this.lib.SYSDG,'SYSKM':this.lib.SYSKM,'SYSPRELIM':this.lib.SYSPRELIM,'SYSRAC':this.lib.SYSRAC};
  
  pooled = true;
  /** if you need to require your lib in runtime and then
   * use `this.lib.methodName()` anywhere and vscode will take care of the dependencies
   * to be installed on a cache folder
   **/
  private get lib(): typeof OracleDBLib {
    const oracledb = this.requireDep('oracledb');
    oracledb.fetchAsString = [oracledb.DATE, oracledb.CLOB, oracledb.NUMBER];
    return oracledb;
  }

  public async open() {
    if (this.connection) {
      if(this.pooled)
      {
        return new Promise<OracleDBLib.Connection>((resolve, reject) => {
          this.lib.getConnection(async (err, conn) => {
            if (err) return reject(err);
            await conn.ping(async error => {
              if (error) return reject(error);
              this.connection = Promise.resolve(conn);
              return resolve(this.connection);
            });
          });
        });
      }
      else{
        let standAloneConnSetting = {
          user: this.credentials.username,
          password: this.credentials.password,
          connectString: this.credentials.connectString,
          privilege: this.privilegeMap[this.privilege]
        }
        return new Promise<OracleDBLib.Connection>((resolve, reject) => {
          this.lib.getConnection(standAloneConnSetting,async (err, conn) => {
            if (err) return reject(err);
            await conn.ping(async error => {
              if (error) return reject(error);
              this.connection = Promise.resolve(conn);
              return resolve(this.connection);
            });
          });
        });
      }
    }
    if(!this.credentials.connectString){
      if (this.credentials.server && this.credentials.port) {
        this.credentials.connectString = `${this.credentials.server}:${this.credentials.port}/${this.credentials.database}`;
      } else {
        this.credentials.connectString = this.credentials.database;
      }
    }
    if(this.credentials.oracleOptions){
      if(this.credentials.oracleOptions.autoCommit){
        this.autoCommit = this.credentials.oracleOptions.autoCommit;
      }
      if(this.credentials.oracleOptions.lowerCase){
        this.lowerCase = this.credentials.oracleOptions.lowerCase;
      }
      if(this.credentials.oracleOptions.macroFile){
        this.macroFile = this.credentials.oracleOptions.macroFile;
      }
      if(this.credentials.oracleOptions.thickMode){
        this.lib.initOracleClient();
      }
      if(this.credentials.oracleOptions.limitPrefetchRows){
        this.maxRows = this.credentials.previewLimit;
      }
      if(this.credentials.oracleOptions.privilege){
        this.privilege = this.credentials.oracleOptions.privilege;
      }
      // if(this.credentials.oracleOptions.pool){
      //   this.pooled = this.credentials.oracleOptions.pool;
      // }
      // if(this.privilege != 'Normal'){
        this.pooled = false;
      // }
    }
    if(this.pooled){
      const pool = await this.lib.createPool({
        user: this.credentials.username,
        password: this.credentials.password,
        connectString: this.credentials.connectString,
        poolIncrement : 0,
        poolMax       : 4,
        poolMin       : 4
      });
      return new Promise<OracleDBLib.Connection>((resolve, reject) => {
        this.lib.getConnection(async (err, conn) => {
          if (err) return reject(err);
          await conn.ping(async error => {
            if (error) return reject(error);
            this.connection = Promise.resolve(conn);
            return resolve(this.connection);
          });
        });
      });
    }else{
      let standAloneConnSetting = {
        user: this.credentials.username,
        password: this.credentials.password,
        connectString: this.credentials.connectString,
        privilege: this.privilegeMap[this.privilege]
      }
      return new Promise<OracleDBLib.Connection>((resolve, reject) => {
        this.lib.getConnection(standAloneConnSetting,async (err, conn) => {
          if (err) return reject(err);
          await conn.ping(async error => {
            if (error) return reject(error);
            this.connection = Promise.resolve(conn);
            return resolve(this.connection);
          });
        });
      });
    }
  }

  public async close() {
    if (!this.connection) return Promise.resolve();
    return this.connection.then((conn) => {
      if(this.pooled){
        return new Promise<void>((resolve, reject) => {
          this.lib.getPool().close(0,(err) => {
            if (err) return reject(err);
            this.connection = null;
            return resolve();
          });
        });
      }
      else{
        return new Promise<void>((resolve, reject) => {
          conn.close((err) => {
            if (err) return reject(err);
            this.connection = null;
            return resolve();
          });
        });
      }
    });
  }

  public query: (typeof AbstractDriver)['prototype']['query'] = async (query, opt = {}) => {
    const messages = [];
    const { requestId } = opt;
    let orcConn: OracleDBLib.Connection;
    return await this.open()
      .then(async (conn: OracleDBLib.Connection) => {
        orcConn = conn;
        const resultsAgg: NSDatabase.IResult[] = [];
        const parseQueries = parse(query.toString());
        
        const options = {
          outFormat: this.lib.OUT_FORMAT_OBJECT,
          dmlRowCounts: true,
          autoCommit: this.autoCommit,
          maxRows: this.maxRows
        };

        await conn.execute(`
          BEGIN
            DBMS_OUTPUT.ENABLE(NULL);
          END;`);

        for (const q of parseQueries.queries) {
          const res: any = await conn.execute(q,{},options) || [];

          if(res.rowsAffected != undefined){
            messages.push(this.prepareMessage(`${res.rowsAffected} rows were affected.`));
            resultsAgg.push(<NSDatabase.IResult>{
              requestId,
              resultId: generateId(),
              connId: this.getId(),
              cols: ['rowsAffted'],
              messages,
              query: q,
              results: [{'rowsAffted':res.rowsAffected+' rows were affected.'}],
            });
          } else if(res.rows){
            resultsAgg.push(<NSDatabase.IResult>{
              requestId,
              resultId: generateId(),
              connId: this.getId(),
              cols: (res.rows && res.rows.length>0) ? Object.keys(res.rows[0]) : [],
              messages,
              query: q,
              results: res.rows,
            });
          } else if (Object.keys(res).length === 0) {
            let outputResult = '';
            let getLineRes;
            do {
              getLineRes = await conn.execute(
                `BEGIN
                  DBMS_OUTPUT.GET_LINE(:ln, :st);
                  END;`,
                  { ln: { dir: this.lib.BIND_OUT, type: this.lib.STRING, maxSize: 32767 },
                    st: { dir: this.lib.BIND_OUT, type: this.lib.NUMBER }
                  }
              );
              if (getLineRes.outBinds.st === 0)
                outputResult += (getLineRes.outBinds.ln + "\n") ;
            } while (getLineRes.outBinds.st === 0);

            messages.push(this.prepareMessage(outputResult));
            resultsAgg.push(<NSDatabase.IResult>{
              requestId,
              resultId: generateId(),
              connId: this.getId(),
              cols: ['DBMS_OUTPUT'],
              messages,
              query: q,
              results: [{'DBMS_OUTPUT':outputResult}],
            });
          }
        }

        return resultsAgg;
      })
      .catch((err) => {
        return [<NSDatabase.IResult>{
          connId: this.getId(),
          requestId,
          resultId: generateId(),
          cols: [],
          messages: messages.concat([
            this.prepareMessage ([
              (err && err.message || err),
              err && err.routine === 'scanner_yyerror' && err.position ? `at character ${err.position}` : undefined
            ].filter(Boolean).join(' '))
          ]),
          error: true,
          rawError: err,
          query,
          results: [],
        }];
      })
      .finally(() => {
        orcConn.close();
      });
  }

  /** if you need a different way to test your connection, you can set it here.
   * Otherwise by default we open and close the connection only
   */
  public async testConnection() {
    await this.open();
    await this.query('SELECT 1 FROM DUAL', {});
  }

  /**
   * This method is a helper to generate the connection explorer tree.
   * it gets the child items based on current item
   */
  public async getChildrenForItem({ item, parent }: Arg0<IConnectionDriver['getChildrenForItem']>) {
    switch (item.type as string) {
      case ContextValue.CONNECTION:
      case ContextValue.CONNECTED_CONNECTION:
        return <MConnectionExplorer.IChildItem[]>[
          { label: 'Tables', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.TABLE },
          { label: 'Views', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.VIEW },
          { label: 'Indexes', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: 'INDEX' },
          { label: 'Packages', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: 'PACKAGE' },
          { label: 'Procedures', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: 'PROCEDURE' },
          { label: 'Functions', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.FUNCTION },
        ];
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        return this.queryResults(this.queries.fetchColumns(item as NSDatabase.ITable));
      case 'PACKAGE':
        return this.queryResults(this.queries.fetchPackageDetails(item as NSDatabase.ISchema));
      case 'PROCEDURE':
      case ContextValue.FUNCTION:
        return this.queryResults(this.queries.fetchProcedureDetails(item as NSDatabase.ISchema));
      case ContextValue.RESOURCE_GROUP:
        return this.getChildrenForGroup({ item, parent });
    }
    return [];
  }

  /**
   * This method is a helper to generate the connection explorer tree.
   * It gets the child based on child types
   */
  private async getChildrenForGroup({ parent, item }: Arg0<IConnectionDriver['getChildrenForItem']>) {
    switch (item.childType as string) {
      case ContextValue.TABLE:
        return this.queryResults(this.queries.fetchTables(parent as NSDatabase.ISchema));
      case ContextValue.VIEW: 
        return this.queryResults(this.queries.fetchViews(parent as NSDatabase.ISchema));
      case ContextValue.FUNCTION:
        return this.queryResults(this.queries.fetchFunctions(parent as NSDatabase.ISchema));
      case 'INDEX':
        return this.queryResults(this.queries.fetchIndexes(parent as NSDatabase.ISchema));
      case 'PACKAGE':
        return this.queryResults(this.queries.fetchPackages(parent as NSDatabase.ISchema));
      case 'PROCEDURE':
        return this.queryResults(this.queries.fetchProcedures(parent as NSDatabase.ISchema));
    }
    return [];
  }
    /**
   * This method is a helper for intellisense and quick picks.
   */
  public async searchItems(itemType: ContextValue, search: string, extraParams: any = {}): Promise<NSDatabase.SearchableItem[]> {
    switch (itemType) {
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        return this.queryResults(this.queries.searchTables({ search })).then(r => r.map(t => {
          if(this.lowerCase){
            t.label = t.label.toLowerCase();
          }
          t.isView = toBool(t.isView);
          return t;
        }));
      case ContextValue.COLUMN:
        return this.queryResults(this.queries.searchColumns({ search, ...extraParams })).then(r => r.map(c => {
          if(this.lowerCase){
            c.label = c.label.toLowerCase();
          }
          c.isPk = toBool(c.isPk);
          c.isFk = toBool(c.isFk);
          return c;
        }));
    }
  }

  public getStaticCompletions: IConnectionDriver['getStaticCompletions'] = async () => {
    return {};
  }
}
