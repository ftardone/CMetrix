using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


using Microsoft.Xrm.Client;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using System.Net;
using Microsoft.Xrm.Client.Services;
using System.Net.Security;
using System.ServiceModel.Description;

namespace CMTX.NUGET.Comunes
{
    public class conexionCRM
    {
        public string CredencialUsuario { get; set; }
        public string CredencialContrasena { get; set; }
        public string CredencialDominio { get; set; }
        public string CRMOrganizationName { get; set; }
        public string CRMServidor { get; set; }
        public string UsaSSL { get; set; }
        public string TipoAutenticacion { get; set; }
        public string http { get; set; }
        public string TipoCRM { get; set; }
        public string CertificateValidation { get; set; }
        public string TimeOut { get; set; }
        public string IFD { get; set; }
        public void getAppConfig(string prefijo)
        { 
            this.CredencialUsuario = ConfigurationManager.AppSettings[prefijo + "_CredencialUsuario"];
            this.CredencialContrasena = ConfigurationManager.AppSettings[prefijo + "_CredencialContrasena"];
            this.CredencialDominio = ConfigurationManager.AppSettings[prefijo + "_CredencialDominio"];
            this.CRMOrganizationName = ConfigurationManager.AppSettings[prefijo + "_CRMOrganizationName"];
            this.CRMServidor = ConfigurationManager.AppSettings[prefijo + "_CRMServidor"];
            this.UsaSSL = ConfigurationManager.AppSettings[prefijo + "_UsaSSL"];
            this.TipoAutenticacion = ConfigurationManager.AppSettings[prefijo + "_TipoAutenticacion"];
            this.http = ConfigurationManager.AppSettings[prefijo + "_TipoLogueo"];
            this.TipoCRM = ConfigurationManager.AppSettings[prefijo + "_TipoCRM"];
            this.CertificateValidation = ConfigurationManager.AppSettings[prefijo + "_CertificateValidation"];
            this.TimeOut = ConfigurationManager.AppSettings[prefijo + "_TimeOut"];
            this.IFD = ConfigurationManager.AppSettings[prefijo + "_IFD"];        
        }
        public IOrganizationService crea_ServicioCRM()
        {
            bool esIFD = Convert.ToBoolean(this.IFD);

            ClientCredentials Credencial = new ClientCredentials();
            if (esIFD)
            {
                Credencial.Windows.ClientCredential = System.Net.CredentialCache.DefaultNetworkCredentials;
                Credencial.Windows.AllowedImpersonationLevel = System.Security.Principal.TokenImpersonationLevel.Impersonation;
                Credencial.UserName.UserName = CredencialDominio + @"\" + CredencialUsuario;
                Credencial.UserName.Password = CredencialContrasena;
            }
            else
            {
                System.Net.NetworkCredential credenciales = new System.Net.NetworkCredential(CredencialUsuario, CredencialContrasena, CredencialDominio);
                Credencial.Windows.ClientCredential = credenciales;
            }

            Uri HomeRealmUri;
            HomeRealmUri = null;

            if (http.Equals("http"))
            {
                if (TipoCRM == "OnLine")
                {
                    CrmConnection conexion = CrmConnection.Parse("Url=https://" + CRMServidor + "; Username=" + CredencialUsuario + "; Password=" + CredencialContrasena + ";");
                    int int_timeout = 0;
                    if (int.TryParse(TimeOut, out int_timeout)) conexion.Timeout = new TimeSpan(0, int_timeout, 0);
                    OrganizationService OService = new OrganizationService(conexion);
                    return ((IOrganizationService)OService);
                }
                else
                {
                    Uri OrganizationUri = new Uri("http://" + CRMServidor + "/" + CRMOrganizationName + "/XRMServices/2011/Organization.svc");

                    OrganizationServiceProxy _serviceProxy;
                    _serviceProxy = new OrganizationServiceProxy(OrganizationUri, HomeRealmUri, Credencial, null);

                    //DESCOMENTAR
                    //_serviceProxy.ServiceConfiguration.CurrentServiceEndpoint.Behaviors.Add(new ProxyTypesBehavior());
                    return ((IOrganizationService)_serviceProxy);
                }
            }
            else
            {
                if (CertificateValidation.Length > 0)
                {
                    bool certificadoValidacion = Convert.ToBoolean(CertificateValidation);
                    Uri OrganizationUri = new Uri("https://" + CRMServidor + "/XRMServices/2011/Organization.svc");

                    if (certificadoValidacion)
                    {
                        ServicePointManager.ServerCertificateValidationCallback = new RemoteCertificateValidationCallback(CertificateValidationCallBack);
                    }
                    else
                    {
                        ServicePointManager.ServerCertificateValidationCallback = new RemoteCertificateValidationCallback(CertificateValidationCallBackTrue);
                    }

                    OrganizationServiceProxy _serviceProxy;
                    _serviceProxy = new OrganizationServiceProxy(OrganizationUri, null, Credencial, null);
                    //DESCOMENTAR
                    //_serviceProxy.ServiceConfiguration.CurrentServiceEndpoint.Behaviors.Add(new ProxyTypesBehavior());


                    return ((IOrganizationService)_serviceProxy);
                }
                else
                {
                    return null;
                }
            }
        }

        private static bool CertificateValidationCallBack(object sender, System.Security.Cryptography.X509Certificates.X509Certificate certificate, System.Security.Cryptography.X509Certificates.X509Chain chain, System.Net.Security.SslPolicyErrors sslPolicyErrors)
        {
            if (sslPolicyErrors == System.Net.Security.SslPolicyErrors.None)
            {
                return true;
            }

            if ((sslPolicyErrors & System.Net.Security.SslPolicyErrors.RemoteCertificateChainErrors) != 0)
            {
                if (chain != null && chain.ChainStatus != null)
                {
                    foreach (System.Security.Cryptography.X509Certificates.X509ChainStatus status in chain.ChainStatus)
                    {
                        if ((certificate.Subject == certificate.Issuer) &&
                           (status.Status == System.Security.Cryptography.X509Certificates.X509ChainStatusFlags.UntrustedRoot))
                        {
                            continue;
                        }
                        else
                        {
                            if (status.Status != System.Security.Cryptography.X509Certificates.X509ChainStatusFlags.NoError)
                            {
                                return false;
                            }
                        }
                    }
                }
                return true;
            }
            else
            {
                return false;
            }
        }

        private static bool CertificateValidationCallBackTrue(object sender, System.Security.Cryptography.X509Certificates.X509Certificate certificate, System.Security.Cryptography.X509Certificates.X509Chain chain, System.Net.Security.SslPolicyErrors sslPolicyErrors)
        {
            return true;
        }

    }
}
