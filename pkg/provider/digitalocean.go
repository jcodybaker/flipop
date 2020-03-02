package provider

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/digitalocean/godo"
	"golang.org/x/oauth2"
)

const (
	DigitalOcean = "digitalocean"
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
func NewDigitalOcean() Provider {
	token := os.Getenv("DIGITALOCEAN_ACCESS_TOKEN")
	if token == "" {
		return nil
	}
	tokenSource := &doTokenSource{
		AccessToken: token,
	}

	oauthClient := oauth2.NewClient(context.Background(), tokenSource)
	client := godo.NewClient(oauthClient)
	return &digitalOcean{client}
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
	if err != nil {
		return fmt.Errorf("parsing provider id: %w", err)
	}
	_, res, err := do.FloatingIPActions.Assign(ctx, ip, int(dropletID))
	if err != nil {
		if res != nil {
			if res.StatusCode == http.StatusNotFound {
				return ErrNotFound
			}
			if res.StatusCode == http.StatusUnprocessableEntity {
				// Typically this means the droplet already has a Floating IP. We will get this
				// error even if we try to assign it the IP it already has.

				// Check the IP's current provider.  If this fails, eat the error and return the
				// assign error.
				curProvider, _ := do.IPtoProviderID(ctx, ip)
				if curProvider != "" && curProvider == providerID {
					return nil // Already set to us. This is success.
				}
			}
		}
		return err
	}
	return nil
}

func (do *digitalOcean) NodeToIP(ctx context.Context, providerID string) (string, error) {
	dropletID, err := strconv.ParseInt(strings.TrimPrefix(providerID, "digitalocean://"), 10, 0)
	if err != nil {
		return "", fmt.Errorf("parsing provider id: %w", err)
	}
	droplet, res, err := do.Droplets.Get(ctx, int(dropletID))
	if err != nil {
		if res != nil && res.StatusCode == http.StatusNotFound {
			return "", ErrNotFound
		}
		return "", err
	}

	if droplet.Networks == nil {
		return "", errors.New("droplet had no networking info")
	}

	var ips []string
	// Floating IPs are listed among node IPs. Likely last.
	for _, v4 := range droplet.Networks.V4 {
		if v4.Type == "public" {
			ips = append(ips, v4.IPAddress)
		}
	}

	if len(ips) < 2 {
		// All droplets have a public IP. With a FLIP, they'll have 2. If there's only 1, no FLIP.
		return "", nil
	}

	// There's nothing explicitly saying the order of IPs, but it seems the FLIP is normally second.
	for i := len(ips) - 1; i >= 0; i++ {
		ip := ips[i]
		ipProvider, err := do.IPtoProviderID(ctx, ip)
		if err == ErrNotFound {
			continue
		}
		if err != nil {
			return "", fmt.Errorf("retrieving node ip: %w", err)
		}
		if ipProvider == providerID {
			return ip, nil
		}
	}

	return "", nil
}
