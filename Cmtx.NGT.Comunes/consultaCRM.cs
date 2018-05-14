using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using System.IO;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using System.Net;
using System.Data;
using Microsoft.Crm.Sdk.Messages;

namespace Cmtx.NGT.Comunes
{
    public class consultaCRM
    {
        IOrganizationService servicio = null;
        public string prefijo = ConfigurationManager.AppSettings["prefijo"];
        public string strErrMessage = "";
        private List<DateTime> feriados = null;
        StringBuilder traza = new StringBuilder();

        #region constructors
        public consultaCRM(string prefijo)
        {
            conexionCRM conCRM = new conexionCRM();
            conCRM.getAppConfig(prefijo);
            this.servicio = conCRM.crea_ServicioCRM();
        }
        public consultaCRM(IOrganizationService _servicio)
        {
            this.servicio = _servicio;
        }

        public class ValorTexto
        {
            public string ValorState { get; set; }
            public string Valor { get; set; }
            public string Texto { get; set; }
        }

        public class ListaValorTexto
        {
            public List<ValorTexto> listaValorTexto { get; set; }
        }
        #endregion

        #region devuelve servicio,guid, diccionarios etc
        public IOrganizationService devuelve_Servicio()
        {
            return this.servicio;
        }
        Guid DevuelveGuid(Entity ent, string atributo)
        {
            Guid resp = Guid.Empty;
            if (!ent.Contains(atributo)) return resp;
            System.Type tipo = ent.Attributes[atributo].GetType();
            if (tipo != typeof(Guid))
            {
                EntityReference entref = (EntityReference)ent.Attributes[atributo];
                resp = entref.Id;
            }
            else
            {
                resp = new Guid(ent.Attributes[atributo].ToString());
            }

            return resp;
        }
        public List<Guid> devuelve_ListaGuid(string strEntidad, FilterExpression filtro)
        {
            List<Guid> resp = new List<Guid>();
            try
            {
                QueryExpression qry = new QueryExpression(strEntidad);
                qry.ColumnSet = new ColumnSet(strEntidad + "id");
                if (filtro!=null) qry.Criteria.AddFilter(filtro);            
                EntityCollection ec = this.servicio.RetrieveMultiple(qry);
                if (ec.Entities.Count > 0)
                {
                    foreach (Entity item in ec.Entities)
                    {
                        Guid IDx = Guid.Empty;
                        Guid.TryParse(item.Attributes[strEntidad + "id"].ToString(), out IDx);                            
                        resp.Add(IDx);
                    }
                }
            }
            catch (Exception err)
            {
                this.strErrMessage = err.Message;
                traza.AppendLine(err.Message);
                return resp;
            }
            return resp;
        }

        public Dictionary<Guid, string> devuelve_DiccionarioEnt(string strEntidad, string campo, FilterExpression filtro)
        {
            Dictionary<Guid, string> resp = new Dictionary<Guid,string>();
            try
            {
                QueryExpression qry = new QueryExpression(strEntidad);
                ColumnSet cols = new ColumnSet();
                cols.AddColumn(strEntidad + "id");
                cols.AddColumn(campo);
                qry.ColumnSet = cols;

                if (filtro != null) qry.Criteria.AddFilter(filtro);
                EntityCollection ec = this.servicio.RetrieveMultiple(qry);
                if (ec.Entities.Count > 0)
                {
                    foreach (Entity item in ec.Entities)
                    {
                        string valoCampo = string.Empty;
                        Guid IDx = Guid.Empty;
                        Guid.TryParse(item.Attributes[strEntidad + "id"].ToString(), out IDx);
                        if (item.Contains(campo))
                        {
                            valoCampo = item.Attributes[campo].ToString();
                        }
                        if (IDx != Guid.Empty && !string.IsNullOrEmpty(valoCampo))
                        {
                            resp.Add(IDx, valoCampo);
                        }
                    }
                }
            }
            catch (Exception err)
            {
                this.strErrMessage = err.Message;
                traza.AppendLine(err.Message);
                return resp;
            }
            return resp;
        }
        public Dictionary<int,string> devuelve_picklist(string entityName, string attributeName)
        {
            Dictionary<int, string> respuesta = new Dictionary<int, string>();
            RetrieveAttributeRequest retrieveAttributeRequest = new RetrieveAttributeRequest();
            retrieveAttributeRequest.EntityLogicalName = entityName;
            retrieveAttributeRequest.LogicalName = attributeName;
            retrieveAttributeRequest.RetrieveAsIfPublished = true;
            RetrieveAttributeResponse retrieveAttributeResponse = (RetrieveAttributeResponse)this.servicio.Execute(retrieveAttributeRequest);
            PicklistAttributeMetadata picklistAttributeMetadata = (PicklistAttributeMetadata)retrieveAttributeResponse.AttributeMetadata;
            OptionSetMetadata optionsetMetadata = picklistAttributeMetadata.OptionSet;
            try
            {
                foreach (OptionMetadata optionMetadata in optionsetMetadata.Options)
                {
                    respuesta.Add(int.Parse(optionMetadata.Value.ToString()), optionMetadata.Label.UserLocalizedLabel.Label);

                }
                return respuesta;
            }
            catch (Exception err)
            {
                this.strErrMessage = err.Message;
                traza.AppendLine(err.Message);
                return respuesta;
            }
        }

        public List<OptionMetadata> devuelve_picklist_meta(string entityName, string attributeName)
        {
            List<OptionMetadata> respuesta = new List<OptionMetadata>();
            RetrieveAttributeRequest retrieveAttributeRequest = new RetrieveAttributeRequest();
            retrieveAttributeRequest.EntityLogicalName = entityName;
            retrieveAttributeRequest.LogicalName = attributeName;
            retrieveAttributeRequest.RetrieveAsIfPublished = true;
            RetrieveAttributeResponse retrieveAttributeResponse = (RetrieveAttributeResponse)this.servicio.Execute(retrieveAttributeRequest);
            PicklistAttributeMetadata picklistAttributeMetadata = (PicklistAttributeMetadata)retrieveAttributeResponse.AttributeMetadata;
            OptionSetMetadata optionsetMetadata = picklistAttributeMetadata.OptionSet;
            respuesta = optionsetMetadata.Options.ToList();
            return respuesta;
        }
        #endregion

        #region multiples

        public Guid registroCreado_GUID(OrganizationRequest organizationRequest, OrganizationResponse organizationResponse)
        {
            Guid resultID = Guid.Empty;
            Guid.TryParse(organizationResponse.Results["id"].ToString(), out resultID);
            return resultID;
        }
        public string registroCreado_Error(OrganizationRequest organizationRequest, int count, string accion, OrganizationServiceFault organizationServiceFault)
        {
            return organizationServiceFault.Message;
        }
        public static List<List<T>> Split<T>(List<T> source, int bloqueSize)
        {
            return source
                .Select((x, i) => new { Index = i, Value = x })
                .GroupBy(x => x.Index / bloqueSize)
                .Select(x => x.Select(v => v.Value).ToList())
                .ToList();
        }

        #endregion

        #region calendario
        public List<DateTime> cal_DevuelveFeriados()
        {
            List<DateTime> resp = new List<DateTime>();
            EntityCollection organizationCollection = servicio.RetrieveMultiple(
                            new QueryExpression("organization")
                            {
                                ColumnSet = new ColumnSet("businessclosurecalendarid")
                            });
            Entity myOrganization = organizationCollection.Entities.FirstOrDefault();
            Guid calendarId = Guid.Parse(myOrganization.Attributes["businessclosurecalendarid"].ToString());
            QueryExpression query = new QueryExpression("calendar");
            query.ColumnSet = new ColumnSet(true);
            ConditionExpression condition = new ConditionExpression();
            condition.AttributeName = "calendarid";
            condition.Operator = ConditionOperator.Equal;
            condition.Values.Add(calendarId);
            query.Criteria.Conditions.Add(condition);
            EntityCollection calendars = servicio.RetrieveMultiple(query);
            Entity businessClosureCalendarEntity = calendars.Entities.FirstOrDefault();
            EntityCollection collectionCalendarRules = businessClosureCalendarEntity.GetAttributeValue<EntityCollection>("calendarrules");
            foreach (Entity e in collectionCalendarRules.Entities)
            {
                DateTime dtferiado = (DateTime)e["starttime"];
                resp.Add(dtferiado);
            }
            return resp;
        }

        public bool cal_es_empresa_cerrada(DateTime fecha)
        {
            if (feriados == null)
            {
                feriados = cal_DevuelveFeriados();
            }

            DateTime result = DateTime.MinValue;
            result = feriados.Find(item => item == fecha);

            return result != DateTime.MinValue ? true : false;
        }

        public Entity cal_devuelve_feriados()
        {
            EntityCollection organizationCollection = servicio.RetrieveMultiple(new QueryExpression("organization") { ColumnSet = new ColumnSet("businessclosurecalendarid", "name") });
            Entity myOrganization                   = organizationCollection.Entities.FirstOrDefault();
            QueryExpression query                   = new QueryExpression("calendar");
            query.ColumnSet                         = new ColumnSet(true);
            ConditionExpression condition           = new ConditionExpression();
            condition.AttributeName                 = "calendarid";
            condition.Operator                      = ConditionOperator.Equal;
            condition.Values.Add((Guid)myOrganization.Attributes["businessclosurecalendarid"]);
            query.Criteria.Conditions.Add(condition);
            EntityCollection ec_calendars           = servicio.RetrieveMultiple(query);
            Entity e_businessClosureCalendar        = ec_calendars.Entities.FirstOrDefault();
            //EntityCollection ec_CalendarRules       = e_businessClosureCalendar.GetAttributeValue<EntityCollection>("calendarrules");

            return e_businessClosureCalendar;
        }

        public DateTime cal_AvanzaXdiasHabiles(DateTime dt, int dias, bool wkSabado, bool wkDomingo)
        {
            if (feriados == null)
            {
                feriados = cal_DevuelveFeriados();
            }
            bool fin = false;
            int cuenta = 0;
            DateTime dia_actual = cal_NormalizaFecha(dt);
            bool descansa = false;
            while (!fin)
            {
                dia_actual = dia_actual.AddDays(1);
                descansa = false;
                //revisa si es feriado
                var result = feriados.Find(item => item == dia_actual);
                if (result != DateTime.MinValue)
                {
                    descansa = true;
                }
                //si es domingo y no se trabaja domingo, consideralo feriado
                if (dia_actual.DayOfWeek == DayOfWeek.Sunday && wkDomingo == false)
                {
                    descansa = true;
                }
                //si es sabado y no se trabaja sabado, consideralo feriado
                if (dia_actual.DayOfWeek == DayOfWeek.Saturday && wkSabado == false)
                {
                    descansa = true;
                }
                //si no es feriado cuenta ++
                if (descansa == false)
                {
                    cuenta++;
                }
                //si cuenta == dias fin=true                
                if (cuenta == dias)
                {
                    fin = true;
                }
            }
            dia_actual = cal_RestableceHMS(dia_actual, dt);
            return dia_actual;
        }

        DateTime cal_NormalizaFecha(DateTime dtinicial)
        {
            DateTime dtnormalizada = new DateTime(dtinicial.Year, dtinicial.Month, dtinicial.Day, 0, 0, 0);
            return dtnormalizada;
        }

        DateTime cal_RestableceHMS(DateTime dtSLA, DateTime dtinicial)
        {
            DateTime dtnormalizada = new DateTime(dtSLA.Year, dtSLA.Month, dtSLA.Day, dtinicial.Hour, dtinicial.Minute, dtinicial.Second);
            return dtnormalizada;
        }

        public int cal_diasHabilesTranscurridos(DateTime dti, DateTime dtf, bool wkSabado, bool wkDomingo)
        {
            int resp = 0;
            bool fin = false;
            int cuenta = 0;
            int signo = 1;
            DateTime dia_actual = cal_NormalizaFecha(dti);
            DateTime dia_final = cal_NormalizaFecha(dtf);

            if (dia_actual == dia_final) return 0;
            if (dia_actual > dia_final)
            {
                DateTime pivot = dia_actual;
                dia_actual = dia_final;
                dia_final = pivot;
                signo = -1;
            }

            bool descansa = false;
            if (feriados == null)
            {
                feriados = cal_DevuelveFeriados();
            }
            while (!fin)
            {
                dia_actual = dia_actual.AddDays(1);
                descansa = false;
                //revisa si es feriado
                var result = feriados.Find(item => item == dia_actual);
                if (result != DateTime.MinValue)
                {
                    descansa = true;
                }
                //si es domingo y no se trabaja domingo, consideralo feriado
                if (dia_actual.DayOfWeek == DayOfWeek.Sunday && wkDomingo == false)
                {
                    descansa = true;
                }
                //si es sabado y no se trabaja sabado, consideralo feriado
                if (dia_actual.DayOfWeek == DayOfWeek.Saturday && wkSabado == false)
                {
                    descansa = true;
                }
                //si no es feriado cuenta ++
                if (descansa == false)
                {
                    cuenta++;
                }
                //si dia_actual = dia_final fin=true                
                if (dia_actual == dia_final)
                {
                    fin = true;
                    resp = cuenta;
                }
            }
            return resp*signo;
        }

        public DateTime cal_RetrocedeXdiasHabiles(DateTime dt, int dias, bool wkSabado, bool wkDomingo)
        {
            if (feriados == null)
            {
                feriados = cal_DevuelveFeriados();
            }
            bool fin = false;
            int cuenta = 0;
            DateTime dia_actual = cal_NormalizaFecha(dt);
            bool descansa = false;
            while (!fin)
            {
                dia_actual = dia_actual.AddDays(-1);
                descansa = false;
                //revisa si es feriado
                var result = feriados.Find(item => item == dia_actual);
                if (result != DateTime.MinValue)
                {
                    descansa = true;
                }
                //si es domingo y no se trabaja domingo, consideralo feriado
                if (dia_actual.DayOfWeek == DayOfWeek.Sunday && wkDomingo == false)
                {
                    descansa = true;
                }
                //si es sabado y no se trabaja sabado, consideralo feriado
                if (dia_actual.DayOfWeek == DayOfWeek.Saturday && wkSabado == false)
                {
                    descansa = true;
                }
                //si no es feriado cuenta ++
                if (descansa == false)
                {
                    cuenta++;
                }
                //si cuenta == dias fin=true                
                if (cuenta == dias)
                {
                    fin = true;
                }
            }
            dia_actual = cal_RestableceHMS(dia_actual, dt);
            return dia_actual;
        }

        public DateTime cal_Avanza_hastadiaHabil(DateTime dt, bool wkSabado, bool wkDomingo)
        {
            if (feriados == null)
            {
                feriados = cal_DevuelveFeriados();
            }
            bool fin = false;
            int cuenta = 0;
            DateTime dia_actual = cal_NormalizaFecha(dt);
            bool descansa = false;
            while (!fin)
            {
                descansa = false;
                //revisa si es feriado
                var result = feriados.Find(item => item == dia_actual);
                if (result != DateTime.MinValue)
                {
                    descansa = true;
                }
                //si es domingo y no se trabaja domingo, consideralo feriado
                if (dia_actual.DayOfWeek == DayOfWeek.Sunday && wkDomingo == false)
                {
                    descansa = true;
                }
                //si es sabado y no se trabaja sabado, consideralo feriado
                if (dia_actual.DayOfWeek == DayOfWeek.Saturday && wkSabado == false)
                {
                    descansa = true;
                }
                //si no es feriado cuenta ++
                if (descansa == false)
                {
                    fin = true;
                }
                else
                {
                    dia_actual = dia_actual.AddDays(1);
                }
            }
            dia_actual = cal_RestableceHMS(dia_actual, dt);
            return dia_actual;
        }
        #endregion

        #region armar queryexpression
        public ColumnSet getColumnas(List<string> campos, string strEntidad)
        {
            ColumnSet resp = new ColumnSet();
            foreach (string cx in campos)
            {
                if (cx == "*")
                {
                    return new ColumnSet(true);
                }
                resp.AddColumn(cx);
            }
            if (campos.Count == 0) resp.AddColumn(strEntidad.ToLower() + "id");
            return resp;
        }
        public ConditionExpression getCondicion(string attr, ConditionOperator operador, List<object> valor)
        {
            ConditionExpression resp = new ConditionExpression();
            resp.AttributeName = attr;
            resp.Operator = operador;
            foreach (object obj in valor) resp.Values.Add(obj);
            return resp;
        }
        public FilterExpression getFiltro(LogicalOperator tipofiltro, List<ConditionExpression> condiciones)
        {
            FilterExpression resp = new FilterExpression();
            resp.FilterOperator = tipofiltro;
            foreach (ConditionExpression condx in condiciones)
            {
                resp.Conditions.Add(condx);
            }
            return resp;
        }
        public LinkEntity getLinkEntity(string entityFrom, string campoFrom, string entityTo, string campoTo, string alias, List<string> campos, List<FilterExpression> filtros)
        {
            LinkEntity lnkcontact = new LinkEntity();
            ColumnSet cols = new ColumnSet();
            foreach (string cx in campos)
            {
                cols.AddColumn(cx);
            }
            lnkcontact.Columns = cols;
            lnkcontact.EntityAlias = alias;
            lnkcontact.LinkFromEntityName = entityFrom;
            lnkcontact.LinkFromAttributeName = campoFrom;
            lnkcontact.LinkToEntityName = entityTo;
            lnkcontact.LinkToAttributeName = campoTo;
            lnkcontact.JoinOperator = JoinOperator.Inner;
            if (filtros != null)
            {
                foreach (FilterExpression filtrolnk in filtros)
                {
                    lnkcontact.LinkCriteria.AddFilter(filtrolnk);
                }
            }
            return lnkcontact;
        }

        public LinkEntity getLinkEntity(string entityFrom, string campoFrom, string entityTo, string campoTo, string alias, List<string> campos, List<FilterExpression> filtros, List<LinkEntity> sublinkeds)
        {
            LinkEntity lnkcontact = new LinkEntity();
            ColumnSet cols = new ColumnSet();
            foreach (string cx in campos)
            {
                cols.AddColumn(cx);
            }
            lnkcontact.Columns = cols;
            lnkcontact.EntityAlias = alias;
            lnkcontact.LinkFromEntityName = entityFrom;
            lnkcontact.LinkFromAttributeName = campoFrom;
            lnkcontact.LinkToEntityName = entityTo;
            lnkcontact.LinkToAttributeName = campoTo;
            lnkcontact.JoinOperator = JoinOperator.Inner;
            if (filtros != null)
            {
                foreach (FilterExpression filtrolnk in filtros)
                {
                    lnkcontact.LinkCriteria.AddFilter(filtrolnk);
                }
            }
            if (sublinkeds != null)
            {
                foreach (LinkEntity lkx in sublinkeds)
                {
                    lnkcontact.LinkEntities.Add(lkx);
                }
            }
            return lnkcontact;
        }

        #endregion

        #region metodos
        public OrganizationResponse SetState(string entitylogicalname, Guid guid, int state, int status)
        {
            try
            {
                EntityReference moniker = new EntityReference();
                moniker.LogicalName = entitylogicalname;
                moniker.Id = guid;
                OrganizationRequest request = new OrganizationRequest() { RequestName = "SetState" };
                request["EntityMoniker"] = moniker;
                request["State"] = new Microsoft.Xrm.Sdk.OptionSetValue(state);
                request["Status"] = new Microsoft.Xrm.Sdk.OptionSetValue(status);
                return this.servicio.Execute(request);
            }
            catch { return null; }
        }

        public object getAttributeValue(Entity targetEntity, string attributeName, string alias)
        {
            object resp = null;
            if (string.IsNullOrEmpty(attributeName))
            {
                return resp;
            }
            //if (!targetEntity.Contains(attributeName)) return resp;
            if (targetEntity[alias + "." + attributeName] is AliasedValue)
            {
                resp = (targetEntity[alias + "." + attributeName] as AliasedValue).Value;
            }
            else
            {
                resp = targetEntity[attributeName];
            }
            return resp;
        }

        #region CONSULTAS A CRM
        public List<T> getListaEntidad<T>(string strEntidad) where T : Entity
        {
            int queryCount = 5000;
            int pageNumber = 1;

            QueryExpression qry = new QueryExpression(strEntidad);
            qry.Criteria.AddCondition("statecode", ConditionOperator.Equal, 0);
            qry.ColumnSet = new ColumnSet(true);

            qry.PageInfo = new PagingInfo();
            qry.PageInfo.Count = queryCount;
            qry.PageInfo.PageNumber = pageNumber;
            qry.PageInfo.PagingCookie = null;

            List<T> listaValCampo = new List<T>();
            try
            {
                while (true)
                { 
                    EntityCollection ec = this.servicio.RetrieveMultiple(qry);
                    foreach (Entity item in ec.Entities)
                    {
                        T ppc = item.ToEntity<T>();
                        listaValCampo.Add(ppc);
                    }
                    if (ec.MoreRecords)
                    {
                        qry.PageInfo.PageNumber++;
                        qry.PageInfo.PagingCookie = ec.PagingCookie;
                    }
                    else
                    {
                        break;
                    }
                }
                return listaValCampo;
            }
            catch (Exception err)
            {
                this.strErrMessage = err.Message;
                traza.AppendLine(err.Message);
                return listaValCampo;
            }
        }

        public List<T> getListaEntidad<T>(string strEntidad, List<string> campos, FilterExpression filtro, OrderExpression orden) where T : Entity
        {
            int queryCount = 5000;
            int pageNumber = 1;

            QueryExpression qry = new QueryExpression(strEntidad);
            //qry.Criteria.AddCondition("statecode", ConditionOperator.Equal, 0);
            if (filtro!=null) qry.Criteria.AddFilter(filtro);
            if (orden != null) qry.Orders.Add(orden);
            ColumnSet cols_qry = new ColumnSet();
            cols_qry.AddColumns(campos.ToArray());
            qry.ColumnSet = cols_qry;

            qry.PageInfo = new PagingInfo();
            qry.PageInfo.Count = queryCount;
            qry.PageInfo.PageNumber = pageNumber;
            qry.PageInfo.PagingCookie = null;

            List<T> listaValCampo = new List<T>();
            try
            {
                while (true)
                {
                    EntityCollection ec = this.servicio.RetrieveMultiple(qry);
                    foreach (Entity item in ec.Entities)
                    {
                        T ppc = item.ToEntity<T>();
                        listaValCampo.Add(ppc);
                    }
                    if (ec.MoreRecords)
                    {
                        qry.PageInfo.PageNumber++;
                        qry.PageInfo.PagingCookie = ec.PagingCookie;
                        traza.AppendLine(strEntidad + ", leyendo pagina " + qry.PageInfo.PageNumber.ToString());
                    }
                    else
                    {
                        break;
                    }
                }
                return listaValCampo;
            }
            catch (Exception err)
            {
                this.strErrMessage = err.Message;
                traza.AppendLine(err.Message);
                return listaValCampo;
            }
        }

        public List<T> getListaEntidad<T>(string strEntidad, List<string> campos, FilterExpression filtro, OrderExpression orden, int paginas) where T : Entity
        {
            int queryCount = 5000;
            int pageNumber = 1;

            QueryExpression qry = new QueryExpression(strEntidad);
            qry.Criteria.AddCondition("statecode", ConditionOperator.Equal, 0);
            if (filtro != null) qry.Criteria.AddFilter(filtro);
            if (orden != null) qry.Orders.Add(orden);
            ColumnSet cols_qry = new ColumnSet();
            cols_qry.AddColumns(campos.ToArray());
            qry.ColumnSet = cols_qry;

            qry.PageInfo = new PagingInfo();
            qry.PageInfo.Count = queryCount;
            qry.PageInfo.PageNumber = pageNumber;
            qry.PageInfo.PagingCookie = null;

            List<T> listaValCampo = new List<T>();
            try
            {
                while (true)
                {
                    EntityCollection ec = this.servicio.RetrieveMultiple(qry);
                    foreach (Entity item in ec.Entities)
                    {
                        T ppc = item.ToEntity<T>();
                        listaValCampo.Add(ppc);
                    }
                    if (ec.MoreRecords)
                    {
                        qry.PageInfo.PageNumber++;
                        qry.PageInfo.PagingCookie = ec.PagingCookie;
                        traza.AppendLine(strEntidad + ", leyendo pagina " + qry.PageInfo.PageNumber.ToString());
                        if (qry.PageInfo.PageNumber >= paginas) break;
                    }
                    else
                    {
                        break;
                    }
                }
                return listaValCampo;
            }
            catch (Exception err)
            {
                this.strErrMessage = err.Message;
                traza.AppendLine(err.Message);
                return listaValCampo;
            }
        }

        public List<ExecuteMultipleResponse> addEjecutaMultiple<T>( List<T> datos) where T : Entity
        {
            List<ExecuteMultipleResponse> respuesta = new List<ExecuteMultipleResponse>();
            try
            {
                ExecuteMultipleRequest requestWithResults;
                var bloques = Split<T>(datos, 1000);
                int cuentabatch = 1;
                foreach (List<T> lista in bloques)
                {
                    List<T> listax = lista;
                    requestWithResults = new ExecuteMultipleRequest()
                    {
                        Settings = new ExecuteMultipleSettings()
                        {
                            ContinueOnError = true,
                            ReturnResponses = true
                        },
                        Requests = new OrganizationRequestCollection()
                    };

                    traza.AppendLine(string.Format("procesando batch {0}/{1} con {2} regs", cuentabatch.ToString(), bloques.Count.ToString(), listax.Count.ToString()));
                    cuentabatch++;

                    foreach (var Patron in listax)
                    {
                        CreateRequest createRequest = new CreateRequest { Target = Patron };
                        requestWithResults.Requests.Add(createRequest);
                    }
                    ExecuteMultipleResponse response = (ExecuteMultipleResponse)this.servicio.Execute(requestWithResults);
                    respuesta.Add(response);
                }
            }
            catch (Exception err)
            {
                this.strErrMessage=err.Message;
                traza.AppendLine(err.Message);
                return respuesta;
            }
            return respuesta;
        }

        public List<ExecuteMultipleResponse> updEjecutaMultiple<T>( List<T> datos) where T : Entity
        {
            List<ExecuteMultipleResponse> respuesta = new List<ExecuteMultipleResponse>();
            try
            {
                ExecuteMultipleRequest requestWithResults;
                var bloques = Split<T>(datos, 1000);
                int numeroBloque = 1;
                foreach (List<T> lista in bloques)
                {
                    traza.AppendLine("procesando batch " + numeroBloque.ToString());
                    List<T> listax = lista;
                    requestWithResults = new ExecuteMultipleRequest()
                    {
                        Settings = new ExecuteMultipleSettings()
                        {
                            ContinueOnError = true,
                            ReturnResponses = true
                        },
                        Requests = new OrganizationRequestCollection()
                    };
                    foreach (var Patron in listax)
                    {
                        UpdateRequest createRequest = new UpdateRequest { Target = Patron };
                        requestWithResults.Requests.Add(createRequest);
                    }
                    ExecuteMultipleResponse response = (ExecuteMultipleResponse)this.servicio.Execute(requestWithResults);
                    respuesta.Add(response);
                    traza.AppendLine("terminado batch " + numeroBloque.ToString());
                    numeroBloque++;
                }
            }
            catch (Exception err)
            {
                this.strErrMessage=err.Message;
                traza.AppendLine(err.Message);
                return respuesta;
            }

            return respuesta;
        }

        public List<ExecuteMultipleResponse> delEjecutaMultiple<T>( List<T> datos) where T : Entity
        {
            List<ExecuteMultipleResponse> respuesta = new List<ExecuteMultipleResponse>();
            try
            {
                ExecuteMultipleRequest requestWithResults;
                var bloques = Split<T>(datos, 1000);
                int cuenta = 1;
                foreach (List<T> lista in bloques)
                {
                    List<T> listax = lista;
                    requestWithResults = new ExecuteMultipleRequest()
                    {
                        Settings = new ExecuteMultipleSettings()
                        {
                            ContinueOnError = true,
                            ReturnResponses = true
                        },
                        Requests = new OrganizationRequestCollection()
                    };
                    foreach (var Patron in listax)
                    {
                        DeleteRequest deleteRequest = new DeleteRequest { Target = Patron.ToEntityReference() };
                        requestWithResults.Requests.Add(deleteRequest);
                    }
                    traza.AppendLine(string.Format("borrando batch {0}/{1}",cuenta,bloques.Count));
                    cuenta++;
                    ExecuteMultipleResponse response = (ExecuteMultipleResponse)this.servicio.Execute(requestWithResults);
                    respuesta.Add(response);
                }
            }
            catch (Exception err)
            {
                this.strErrMessage = err.Message;
                traza.AppendLine(err.Message);
                return respuesta;
            }
            return respuesta;
        }

        public EntityCollection BuscaEntidad(string NombreEntidad, string[] atributosaObtener, string[] atributosaComparar, object[] ValoresaComparar)
        {
            consultaCRM CRM = new consultaCRM(prefijo);
            QueryExpression query = new QueryExpression(NombreEntidad);
            query.ColumnSet = atributosaObtener[0].Equals("*") ? new ColumnSet(true) : new ColumnSet(atributosaObtener);
            FilterExpression filter = new FilterExpression();

            for (int i = 0; i < atributosaComparar.Length; i++)
            {
                filter.AddCondition(new ConditionExpression(atributosaComparar[i], ConditionOperator.Equal, ValoresaComparar[i]));
            }

            query.Criteria.AddFilter(filter);

            try
            {
                EntityCollection ec = this.servicio.RetrieveMultiple(query);
                return ec;
            }
            catch (Exception err)
            {
                this.strErrMessage = err.Message;
                traza.AppendLine(err.Message);
                return null;
            }
        }
        
        public bool BuscaEntidad(ref EntityCollection entidades, string Atributo, string Valor, string NombreEntidad, string[] ColumnasParaRetornar)
        {
            try
            {
                consultaCRM CRM                 = new consultaCRM(prefijo);
                QueryExpression query           = new QueryExpression(NombreEntidad);
                query.ColumnSet                 = new ColumnSet(ColumnasParaRetornar);
                FilterExpression filter         = new FilterExpression();

                filter.AddCondition(new ConditionExpression(Atributo, ConditionOperator.Equal, Valor));
                query.Criteria.AddFilter(filter);

                entidades = CRM.getQueryEC(query);

                return entidades.Entities.Count > 0 ? true : false;
            }
            catch (Exception ex)
            {
                //string ErrorInterno = ConfigurationManager.AppSettings["ErrorInterno"].ToString();
                //throw new Exception(ErrorInterno);
                return false;
            }
        }

        public bool BuscaEntidad(ref EntityCollection entidades, string NombreEntidad, string[] atributosaComparar, object[] ValoresaComparar, string[] atributosaObtener)
        {
            try
            {
                consultaCRM CRM                 = new consultaCRM(prefijo);
                QueryExpression query           = new QueryExpression(NombreEntidad);
                query.ColumnSet                 = new ColumnSet(atributosaObtener);
                FilterExpression filter         = new FilterExpression();

                for (int i = 0; i < atributosaComparar.Length; i++)
                {
                    filter.AddCondition(new ConditionExpression(atributosaComparar[i], ConditionOperator.Equal, ValoresaComparar[i]));
                }

                query.Criteria.AddFilter(filter);
                entidades = CRM.getQueryEC(query);

                return entidades.Entities.Count > 0 ? true : false;
            }
            catch (Exception ex)
            {
                //logger.Debug("Error en ExisteRutCiudadano : " + ex.Message);
                return false;
            }
        }

        public EntityCollection getListaEntidad(string strEntidad, List<string> campos, FilterExpression filtro, OrderExpression orden) 
        {
            QueryExpression qry = new QueryExpression(strEntidad);
            if (filtro != null) qry.Criteria.AddFilter(filtro);
            if (orden != null) qry.Orders.Add(orden);
            ColumnSet cols_qry = new ColumnSet();
            cols_qry.AddColumns(campos.ToArray());
            qry.ColumnSet = cols_qry;            
            try
            {
                EntityCollection ec = this.servicio.RetrieveMultiple(qry);
                return ec;
            }
            catch (Exception err)
            {
                this.strErrMessage = err.Message;
                traza.AppendLine(err.Message);
                return null;
            }
        }

        public EntityCollection getListaEntidad(string strEntidad, List<string> campos, FilterExpression filtro, OrderExpression orden, List<LinkEntity> linkeds)
        {
            QueryExpression qry = new QueryExpression(strEntidad);
            if (filtro != null) qry.Criteria.AddFilter(filtro);
            if (orden != null) qry.Orders.Add(orden);
            ColumnSet cols_qry = new ColumnSet();
            cols_qry.AddColumns(campos.ToArray());
            qry.ColumnSet = cols_qry;
            if (linkeds != null)
            {
                foreach (LinkEntity lkx in linkeds)
                {
                    qry.LinkEntities.Add(lkx);
                }
            }
            try
            {
                EntityCollection ec = this.servicio.RetrieveMultiple(qry);
                return ec;
            }
            catch (Exception err)
            {
                this.strErrMessage = err.Message;
                traza.AppendLine(err.Message);
                return null;
            }
        }

        public T getEntidad<T>(string strEntidad, Guid entidadID, List<string> campos) where T : Entity
        {
            Entity ent = this.servicio.Retrieve(strEntidad, entidadID, getColumnas(campos, strEntidad));
            T ppc = ent.ToEntity<T>();
            return ppc;
        }

        public Entity getEntidadCon1Filtro(string strEntidad, string filtro_campo, string valor_campo, List<string> campos)
        {
            QueryExpression query               = new QueryExpression(strEntidad);
            query.ColumnSet                     = getColumnas(campos, strEntidad);
            FilterExpression filterExpression   = new FilterExpression();
            filterExpression.AddCondition(new ConditionExpression(filtro_campo, ConditionOperator.Equal, valor_campo));
            query.Criteria.AddFilter(filterExpression);

            EntityCollection ec_response = this.servicio.RetrieveMultiple(query);

            if (ec_response.Entities.Count > 0)
            {
                return ec_response.Entities[0];
            }
            else
            {
                return new Entity();
            }
        }

        public EntityCollection getEntidadesCon1Filtro(string strEntidad, string filtro_campo, string valor_campo, List<string> campos)
        {
            QueryExpression query = new QueryExpression(strEntidad);
            query.ColumnSet = getColumnas(campos, strEntidad);
            FilterExpression filterExpression = new FilterExpression();
            filterExpression.AddCondition(new ConditionExpression(filtro_campo, ConditionOperator.Equal, valor_campo));
            query.Criteria.AddFilter(filterExpression);

            EntityCollection ec_response = this.servicio.RetrieveMultiple(query);

            return ec_response;
        }

        public Entity getEntidadCon2Filtros(string strEntidad, string filtro_campo_1, string valor_campo_1, string filtro_campo_2, string valor_campo_2, List<string> campos)
        {
            QueryExpression query               = new QueryExpression(strEntidad);
            query.ColumnSet                     = getColumnas(campos, strEntidad);
            FilterExpression filterExpression   = new FilterExpression();
            filterExpression.AddCondition(new ConditionExpression(filtro_campo_1, ConditionOperator.Equal, valor_campo_1));
            filterExpression.AddCondition(new ConditionExpression(filtro_campo_2, ConditionOperator.Equal, valor_campo_2));
            query.Criteria.AddFilter(filterExpression);

            EntityCollection ec_response = this.servicio.RetrieveMultiple(query);

            if (ec_response.Entities.Count > 0)
            {
                return ec_response.Entities[0];
            }
            else
            {
                return new Entity();
            }
        }

        public List<Entity> getQuery(string strEntity, ColumnSet cols, List<FilterExpression> filtros, List<OrderExpression> ordenar, List<LinkEntity> linkeds)
        {
            List<Entity> resp = new List<Entity>();
            int queryCount = 5000;
            int pageNumber = 1;
            QueryExpression query = new QueryExpression();
            query.EntityName = strEntity;
            if (cols != null) query.ColumnSet = cols;
            if (ordenar != null)
            {
                foreach (OrderExpression ordx in ordenar)
                {
                    query.Orders.Add(ordx);
                }
            }
            if (filtros != null)
            {
                foreach (FilterExpression fx in filtros)
                {
                    query.Criteria.Filters.Add(fx);
                }
            }
            if (linkeds != null)
            {
                foreach (LinkEntity lkx in linkeds)
                {
                    query.LinkEntities.Add(lkx);
                }
            }
            query.PageInfo = new PagingInfo();
            query.PageInfo.Count = queryCount;
            query.PageInfo.PageNumber = pageNumber;
            query.PageInfo.PagingCookie = null;
            while (true)
            {
                EntityCollection ec = this.servicio.RetrieveMultiple(query);
                foreach (Entity item in ec.Entities)
                {
                    resp.Add(item);
                }
                if (ec.MoreRecords)
                {
                    query.PageInfo.PageNumber++;
                    query.PageInfo.PagingCookie = ec.PagingCookie;
                    traza.AppendLine(strEntity + ", leyendo pagina " + query.PageInfo.PageNumber.ToString());
                }
                else
                {
                    break;
                }
            }
            return resp;
        }

        public List<Entity> getQuery(string strEntity, ColumnSet cols, FilterExpression filtro)
        {
            List<Entity> resp = new List<Entity>();
            int queryCount = 5000;
            int pageNumber = 1;
            QueryExpression query = new QueryExpression();
            query.EntityName = strEntity;
            if (cols != null) query.ColumnSet = cols;
            if (filtro!=null) query.Criteria.Filters.Add(filtro);
            query.PageInfo = new PagingInfo();
            query.PageInfo.Count = queryCount;
            query.PageInfo.PageNumber = pageNumber;
            query.PageInfo.PagingCookie = null;
            while (true)
            {
                EntityCollection ec = this.servicio.RetrieveMultiple(query);
                foreach (Entity item in ec.Entities)
                {
                    resp.Add(item);
                }
                if (ec.MoreRecords)
                {
                    query.PageInfo.PageNumber++;
                    query.PageInfo.PagingCookie = ec.PagingCookie;
                    traza.AppendLine(strEntity + ", leyendo pagina " + query.PageInfo.PageNumber.ToString());
                }
                else
                {
                    break;
                }
            }
            return resp;
        }

        public List<Entity> getQuery(QueryExpression query)
        {
            List<Entity> resp = new List<Entity>();
            int queryCount = 5000;
            int pageNumber = 1;
            
            query.PageInfo = new PagingInfo();
            query.PageInfo.Count = queryCount;
            query.PageInfo.PageNumber = pageNumber;
            query.PageInfo.PagingCookie = null;
            while (true)
            {
                EntityCollection ec = this.servicio.RetrieveMultiple(query);
                foreach (Entity item in ec.Entities)
                {
                    resp.Add(item);
                }
                if (ec.MoreRecords)
                {
                    query.PageInfo.PageNumber++;
                    query.PageInfo.PagingCookie = ec.PagingCookie;
                    traza.AppendLine("leyendo pagina " + query.PageInfo.PageNumber.ToString());
                }
                else
                {
                    break;
                }
            }
            return resp;
        }

        public EntityCollection getQueryEC(QueryExpression query)
        {
            int queryCount              = 5000;
            int pageNumber              = 1;            
            query.PageInfo              = new PagingInfo();
            query.PageInfo.Count        = queryCount;
            query.PageInfo.PageNumber   = pageNumber;
            query.PageInfo.PagingCookie = null;
            EntityCollection ec         = this.servicio.RetrieveMultiple(query);

            return ec;
        }

        #endregion CONSULTAS A CRM
        
        public List<ValorTexto> ListarOpciones(string entityName, string attributeName, bool ConOpcionTodas)
        {
            List<ValorTexto> listaOpciones                      = new List<ValorTexto>();
            RetrieveAttributeRequest retrieveAttributeRequest   = new RetrieveAttributeRequest();
            retrieveAttributeRequest.EntityLogicalName          = entityName;
            retrieveAttributeRequest.LogicalName                = attributeName;
            retrieveAttributeRequest.RetrieveAsIfPublished      = true;

            try
            {
                RetrieveAttributeResponse retrieveAttributeResponse = (RetrieveAttributeResponse)servicio.Execute(retrieveAttributeRequest);
                OptionSetMetadata optionsetMetadata;

                if (attributeName.Equals("statecode"))
                {
                    StateAttributeMetadata stateAttributeMetadata   = (StateAttributeMetadata)retrieveAttributeResponse.AttributeMetadata;
                    OptionSetMetadata optionsetMetadata1            = stateAttributeMetadata.OptionSet;
                    optionsetMetadata                               = optionsetMetadata1;
                }
                else if (attributeName.Equals("statuscode"))
                {
                    StatusAttributeMetadata statusAttributeMetadata = (StatusAttributeMetadata)retrieveAttributeResponse.AttributeMetadata;
                    OptionSetMetadata optionsetMetadata2            = statusAttributeMetadata.OptionSet;
                    optionsetMetadata                               = optionsetMetadata2;
                }
                else
                {
                    PicklistAttributeMetadata picklistAttributeMetadata = (PicklistAttributeMetadata)retrieveAttributeResponse.AttributeMetadata;
                    OptionSetMetadata optionsetMetadata4                = picklistAttributeMetadata.OptionSet;
                    optionsetMetadata                                   = optionsetMetadata4;
                }

                if (ConOpcionTodas)
                {
                    listaOpciones.Add(new ValorTexto() { Valor = "0", Texto = "Todas" });
                }

                foreach (OptionMetadata optionMetadata in optionsetMetadata.Options)
                {
                    if (attributeName.Equals("statuscode"))
                    {
                        listaOpciones.Add(new ValorTexto() { ValorState = ((StatusOptionMetadata)(optionMetadata)).State.ToString(), Valor = optionMetadata.Value.ToString(), Texto = optionMetadata.Label.UserLocalizedLabel.Label });
                    }
                    else
                    {
                        listaOpciones.Add(new ValorTexto() { Valor = optionMetadata.Value.ToString(), Texto = optionMetadata.Label.UserLocalizedLabel.Label });
                    }
                }
                return listaOpciones;


            }
            catch (Exception)
            {
                throw;
            }
        }
        
        public List<ValorTexto> ListarOpcion(string entityName, string attributeName, int attributeValue)
        {
            List<ValorTexto> listaOpciones                      = new List<ValorTexto>();
            RetrieveAttributeRequest retrieveAttributeRequest   = new RetrieveAttributeRequest();
            retrieveAttributeRequest.EntityLogicalName          = entityName;
            retrieveAttributeRequest.LogicalName                = attributeName;
            retrieveAttributeRequest.RetrieveAsIfPublished      = true;

            try
            {
                RetrieveAttributeResponse retrieveAttributeResponse = (RetrieveAttributeResponse)servicio.Execute(retrieveAttributeRequest);
                OptionSetMetadata optionsetMetadata;

                if (attributeName.Equals("statecode"))
                {
                    StateAttributeMetadata stateAttributeMetadata   = (StateAttributeMetadata)retrieveAttributeResponse.AttributeMetadata;
                    OptionSetMetadata optionsetMetadata1            = stateAttributeMetadata.OptionSet;
                    optionsetMetadata                               = optionsetMetadata1;
                }
                else if (attributeName.Equals("statuscode"))
                {
                    StatusAttributeMetadata statusAttributeMetadata = (StatusAttributeMetadata)retrieveAttributeResponse.AttributeMetadata;
                    OptionSetMetadata optionsetMetadata2            = statusAttributeMetadata.OptionSet;
                    optionsetMetadata                               = optionsetMetadata2;
                }
                else
                {
                    PicklistAttributeMetadata picklistAttributeMetadata = (PicklistAttributeMetadata)retrieveAttributeResponse.AttributeMetadata;
                    OptionSetMetadata optionsetMetadata4                = picklistAttributeMetadata.OptionSet;
                    optionsetMetadata                                   = optionsetMetadata4;
                }

                foreach (OptionMetadata optionMetadata in optionsetMetadata.Options)
                {
                    if (optionMetadata.Value == attributeValue)
                    {
                        if (attributeName.Equals("statuscode"))
                        {
                            listaOpciones.Add(new ValorTexto() { ValorState = ((StatusOptionMetadata)(optionMetadata)).State.ToString(), Valor = optionMetadata.Value.ToString(), Texto = optionMetadata.Label.UserLocalizedLabel.Label });
                        }
                        else
                        {
                            listaOpciones.Add(new ValorTexto() { Valor = optionMetadata.Value.ToString(), Texto = optionMetadata.Label.UserLocalizedLabel.Label });
                        }

                        break;
                    }
                }
                return listaOpciones;
            }
            catch (Exception)
            {
                throw;
            }
        }
                
        public static string fn_devuelveGlosa(List<ValorTexto> listadeOpciones, string iValor)
        {

            foreach (ValorTexto item in listadeOpciones)
            {
                if (item.Valor.ToString().Equals(iValor))
                {
                    return item.Texto.ToString();
                }
            }

            return "";

        }

        public static string fn_devuelveValor(List<ValorTexto> listadeOpciones, string iGlosa)
        {

            foreach (ValorTexto item in listadeOpciones)
            {
                if (item.Texto.ToString().Equals(iGlosa))
                {
                    return item.Valor.ToString();
                }
            }

            return "";

        }

        public Entity[] Devuelve_toFrom(string direcciones)
        {
            string[] words = direcciones.Split(';');
            List<Entity> listap = new List<Entity>();
            foreach (string emil in words)
            {
                if (emil == "") continue;
                Entity ap = new Entity();
                ap = new Entity("activityparty");
                ap["addressused"] = emil;
                listap.Add(ap);
            }
            return listap.ToArray();
        }
        public List<ExecuteMultipleResponse> addEjecutaMultiple_CreateRequest(List<CreateRequest> datos)
        {
            List<ExecuteMultipleResponse> respuesta = new List<ExecuteMultipleResponse>();
            try
            {
                ExecuteMultipleRequest requestWithResults;
                var bloques = Split<CreateRequest>(datos, 1000);
                int cuentabatch = 1;
                foreach (List<CreateRequest> lista in bloques)
                {
                    List<CreateRequest> listax = lista;
                    requestWithResults = new ExecuteMultipleRequest()
                    {
                        Settings = new ExecuteMultipleSettings()
                        {
                            ContinueOnError = true,
                            ReturnResponses = true
                        },
                        Requests = new OrganizationRequestCollection()
                    };
                    traza.AppendLine(string.Format("procesando batch {0}/{1} con {2} regs", cuentabatch.ToString(), bloques.Count.ToString(), listax.Count.ToString()));
                    cuentabatch++;

                    foreach (var Patron in listax)
                    {
                        requestWithResults.Requests.Add(Patron);
                    }
                    ExecuteMultipleResponse response = (ExecuteMultipleResponse)this.servicio.Execute(requestWithResults);
                    respuesta.Add(response);
                }
            }
            catch (Exception err)
            {
                this.strErrMessage = err.Message;
                traza.AppendLine(err.Message);
            }

            return respuesta;
        }

        public void cambia_propietario_user(string entidad, Guid registroID, Guid propietario)
        {
            AssignRequest assign = new AssignRequest
            {
                Assignee = new EntityReference("systemuser", propietario),
                Target = new EntityReference(entidad, registroID)
            };
            this.servicio.Execute(assign);        
        }

        public void asociaNN(string em_name1, Guid em_id1, string em_name2, Guid em_id2, string strEntityRelationshipName)
        {
            EntityReference Moniker1 = new EntityReference();
            Moniker1.Name = em_name1;
            Moniker1.Id = em_id1;

            EntityReference Moniker2 = new EntityReference();
            Moniker2.Name = em_name2;
            Moniker2.Id = em_id2;

            AssociateEntitiesRequest request = new AssociateEntitiesRequest();

            request.Moniker1 = new EntityReference { Id = Moniker1.Id, LogicalName = Moniker1.Name };
            request.Moniker2 = new EntityReference { Id = Moniker2.Id, LogicalName = Moniker2.Name };
            request.RelationshipName = strEntityRelationshipName;

            this.servicio.Execute(request);
        }
        public void desasociaNN(string em_name1, Guid em_id1, string em_name2, Guid em_id2, string strEntityRelationshipName)
        {
            EntityReference Moniker1 = new EntityReference();
            Moniker1.Name = em_name1;
            Moniker1.Id = em_id1;

            EntityReference Moniker2 = new EntityReference();
            Moniker2.Name = em_name2;
            Moniker2.Id = em_id2;

            DisassociateEntitiesRequest request = new DisassociateEntitiesRequest();

            request.Moniker1 = new EntityReference { Id = Moniker1.Id, LogicalName = Moniker1.Name };
            request.Moniker2 = new EntityReference { Id = Moniker2.Id, LogicalName = Moniker2.Name };
            request.RelationshipName = strEntityRelationshipName;

            this.servicio.Execute(request);
        }

        public void cierraCaso( Guid entID, int estado, int razon)
        {
            SetStateRequest state = new SetStateRequest();
            state.State = new OptionSetValue(estado); 
            state.Status = new OptionSetValue(razon);
            state.EntityMoniker = new EntityReference("incident", entID);
            SetStateResponse stateSet = (SetStateResponse)servicio.Execute(state);
        }


        #endregion

        #region auxiliares
        public DataTable entity_collection_to_datatable(EntityCollection ec)
        {
            DataTable dTable = new DataTable();
            string columnName = string.Empty;
            int iElement = 0;
            string colName = string.Empty;
            string colType = string.Empty;

            if (ec.Entities.Count == 0) { return null; }

            try
            {
                for (iElement = 0; iElement <= ec.Entities[0].Attributes.Count - 1; iElement++)
                {
                    columnName = ec.Entities[0].Attributes.Keys.ElementAt(iElement);
                    dTable.Columns.Add(columnName);
                }

                foreach (Entity entity in ec.Entities)
                {
                    DataRow dRow = dTable.NewRow();
                    for (int i = 0; i <= entity.Attributes.Count - 1; i++)
                    {
                        try
                        {
                            colName = entity.Attributes.Keys.ElementAt(i);
                            colType = entity.Attributes.Values.ElementAt(i).ToString();

                            if (dTable.Columns.Contains(colName))
                            {
                                switch (colType)
                                {
                                    case "Microsoft.Xrm.Sdk.AliasedValue":
                                        dRow[colName] = ((AliasedValue)((entity.Attributes.Values.ElementAt(i)))).Value;
                                        break;
                                    case "Microsoft.Xrm.Sdk.OptionSetValue":
                                        dRow[colName] = ((OptionSetValue)((entity.Attributes.Values.ElementAt(i)))).Value;
                                        break;
                                    case "Microsoft.Xrm.Sdk.EntityReference":
                                        dRow[colName] = ((EntityReference)((entity.Attributes.Values.ElementAt(i)))).Id;
                                        break;
                                    default:
                                        dRow[colName] = entity.Attributes.Values.ElementAt(i);
                                        break;
                                }
                            }
                        }
                        catch (Exception ex) 
                        {
                            dRow[colName] = entity.Attributes.Values.ElementAt(i);
                        }
                    }
                    dTable.Rows.Add(dRow);
                }
            }
            catch (Exception es)
            {

                throw;
            }
            return dTable;
        }
        #endregion
    }
}
