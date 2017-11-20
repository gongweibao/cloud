package config

import apiv1 "k8s.io/client-go/tools/clientcmd/api/v1"

func generateClustersSpec(name, server, ca string) []apiv1.NamedCluster {
	return []apiv1.NamedCluster{
		apiv1.NamedCluster{
			Name: name,
			Cluster: apiv1.Cluster{
				Server:               server,
				CertificateAuthority: ca,
			},
		},
	}
}

func generateUsersSpec(name, key, crt string) []apiv1.NamedAuthInfo {
	return []apiv1.NamedAuthInfo{
		apiv1.NamedAuthInfo{
			Name: name,
			AuthInfo: apiv1.AuthInfo{
				ClientKey:         key,
				ClientCertificate: crt,
			},
		},
	}
}

func generateContextSpec(name, user, namespace string) []apiv1.NamedContext {
	return []apiv1.NamedContext{
		apiv1.NamedContext{
			Name: name,
			Context: apiv1.Context{
				Cluster:   name,
				AuthInfo:  user,
				Namespace: namespace,
			},
		},
	}
}

// ClientConfig holds fields to create a custom config.
type ClientConfig struct {
	ClusterName    string
	ServerName     string
	CurrentContext string
	CA             string
	ClientKey      string
	ClientCrt      string
	NameSpace      string
}

func createValidTestConfig(clusterName, server, curContext,
	ca, clientKey, clientCrt string) *apiv1.Config {
	return &apiv1.Config{
		Kind:           "Config",
		APIVersion:     "v1",
		Clusters:       generateClustersSpec(clusterName, server, ca),
		Contexts:       generateContextSpec(curContext, "", ""),
		AuthInfos:      generateUsersSpec(clusterName, clientKey, clientCrt),
		CurrentContext: curContext,
		Preferences:    apiv1.Preferences{},
	}
}
