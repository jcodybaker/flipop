package provider

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/digitalocean/godo"
	"golang.org/x/oauth2"
)

type digitalOcean struct {
	*godo.Client
}

type doTokenSource struct {
	AccessToken string
}

func (t *doTokenSource) Token() (*oauth2.Token, error) {
	token := &oauth2.Token{
		AccessToken: t.AccessToken,
	}
	return token, nil
}

// NewDigitalOcean returns a new provider for DigitalOcean.
func NewDigitalOcean() (Provider, error) {
	tokenSource := &doTokenSource{
		AccessToken: os.Getenv("DIGITALOCEAN_ACCESS_TOKEN"),
	}

	oauthClient := oauth2.NewClient(context.Background(), tokenSource)
	client := godo.NewClient(oauthClient)
	return &digitalOcean{client}, nil
}

func (do *digitalOcean) IPtoProviderID(ctx context.Context, ip string) (string, error) {
	flip, res, err := do.FloatingIPs.Get(ctx, ip)
	if err != nil {
		if res != nil && res.StatusCode == http.StatusNotFound {
			return "", ErrNotFound
		}
		return "", err
	}
	if flip.Droplet == nil {
		return "", nil
	}
	return fmt.Sprintf("digitalocean://%d", flip.Droplet.ID), nil
}

func (do *digitalOcean) AssignIP(ctx context.Context, ip, providerID string) error {
	dropletID, err := strconv.ParseInt(strings.TrimPrefix(providerID, "digitalocean://"), 10, 0)
	_, res, err := do.FloatingIPActions.Assign(ctx, ip, int(dropletID))
	if err != nil {
		if res != nil && res.StatusCode == http.StatusNotFound {
			return ErrNotFound
		}
		return err
	}
	return nil
}
