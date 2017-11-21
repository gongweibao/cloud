package config

import (
	apiv1 "k8s.io/client-go/tools/clientcmd/api/v1"
)

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
	CurrentContext string
	NameSpace      string
}

// K8sConfigReq holds info of request from paddlecloud.
type K8sConfigReq struct {
	Host      string `json:k8s_host`
	CA        string `json:k8s_ca`
	ClientKey string `json:client_key`
	ClientCrt string `json:client_crt`
}

func createValidConfig(clusterName, server, curContext,
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

/*
func getK8sConfig(uri string, token string) (K8sConfigReq, error) {
	authHeader := make(map[string]string)
	authHeader["Authorization"] = token

	str := fmt.Sprintf("get uri with token error uri:%s token:%s\n", uri, token)

	req, err := restclient.MakeRequest(uri, "GET", nil, "", nil, authHeader)
	if err != nil {
		log.Errorln(str)
		return "", err
	}

	body, err := restclient.GetResponse(req)
	if err != nil {
		log.Errorln(str)
		return "", err
	}

	log.V(4).Infoln("get token2user resp:" + string(body[:]))
	var resp K8sConfigReq
	if err := json.Unmarshal(body, &resp); err != nil {
		log.Errorln(string(body[:]))
		return "", err
	}

	return user, nil
}
*/

func saveToFile(filename string) {
}
