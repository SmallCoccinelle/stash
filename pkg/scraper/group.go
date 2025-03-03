package scraper

import (
	"context"
	"fmt"
	"net/http"

	"github.com/stashapp/stash/pkg/models"
)

type group struct {
	config config

	txnManager models.TransactionManager
	globalConf GlobalConfig
}

func newGroupScraper(c config, txnManager models.TransactionManager, globalConfig GlobalConfig) scraper {
	return group{
		config:     c,
		txnManager: txnManager,
		globalConf: globalConfig,
	}
}

func (g group) spec() models.Scraper {
	return g.config.spec()
}

// fragmentScraper finds an appropriate fragment scraper based on input.
func (g group) fragmentScraper(input Input) *scraperTypeConfig {
	switch {
	case input.Performer != nil:
		return g.config.PerformerByFragment
	case input.Gallery != nil:
		// TODO - this should be galleryByQueryFragment
		return g.config.GalleryByFragment
	case input.Scene != nil:
		return g.config.SceneByQueryFragment
	}

	return nil
}

// scrapeFragmentInput analyzes the input and calls an appropriate scraperActionImpl
func scrapeFragmentInput(ctx context.Context, input Input, s scraperActionImpl) (models.ScrapedContent, error) {
	switch {
	case input.Performer != nil:
		return s.scrapePerformerByFragment(*input.Performer)
	case input.Gallery != nil:
		return s.scrapeGalleryByFragment(*input.Gallery)
	case input.Scene != nil:
		return s.scrapeSceneByFragment(ctx, *input.Scene)
	}

	return nil, ErrNotSupported
}

func (g group) viaFragment(ctx context.Context, client *http.Client, input Input) (models.ScrapedContent, error) {
	stc := g.fragmentScraper(input)
	if stc == nil {
		// If there's no performer fragment scraper in the group, we try to use
		// the URL scraper. Check if there's an URL in the input, and then shift
		// to an URL scrape if it's present.
		if input.Performer != nil && input.Performer.URL != nil && *input.Performer.URL != "" {
			return g.viaURL(ctx, client, *input.Performer.URL, models.ScrapeContentTypePerformer)
		}

		return nil, ErrNotSupported
	}

	s := g.config.getScraper(*stc, client, g.txnManager, g.globalConf)
	return scrapeFragmentInput(ctx, input, s)
}

func (g group) viaScene(ctx context.Context, client *http.Client, scene *models.Scene) (*models.ScrapedScene, error) {
	if g.config.SceneByFragment == nil {
		return nil, ErrNotSupported
	}

	s := g.config.getScraper(*g.config.SceneByFragment, client, g.txnManager, g.globalConf)
	return s.scrapeSceneByScene(ctx, scene)
}

func (g group) viaGallery(ctx context.Context, client *http.Client, gallery *models.Gallery) (*models.ScrapedGallery, error) {
	if g.config.GalleryByFragment == nil {
		return nil, ErrNotSupported
	}

	s := g.config.getScraper(*g.config.GalleryByFragment, client, g.txnManager, g.globalConf)
	return s.scrapeGalleryByGallery(ctx, gallery)
}

func loadUrlCandidates(c config, ty models.ScrapeContentType) []*scrapeByURLConfig {
	switch ty {
	case models.ScrapeContentTypePerformer:
		return c.PerformerByURL
	case models.ScrapeContentTypeScene:
		return c.SceneByURL
	case models.ScrapeContentTypeMovie:
		return c.MovieByURL
	case models.ScrapeContentTypeGallery:
		return c.GalleryByURL
	}

	panic("loadUrlCandidates: unreachable")
}

func scrapeByUrl(ctx context.Context, url string, s scraperActionImpl, ty models.ScrapeContentType) (models.ScrapedContent, error) {
	switch ty {
	case models.ScrapeContentTypePerformer:
		return s.scrapePerformerByURL(ctx, url)
	case models.ScrapeContentTypeScene:
		return s.scrapeSceneByURL(ctx, url)
	case models.ScrapeContentTypeMovie:
		return s.scrapeMovieByURL(ctx, url)
	case models.ScrapeContentTypeGallery:
		return s.scrapeGalleryByURL(ctx, url)
	}

	panic("scrapeByUrl: unreachable")
}

func (g group) viaURL(ctx context.Context, client *http.Client, url string, ty models.ScrapeContentType) (models.ScrapedContent, error) {
	candidates := loadUrlCandidates(g.config, ty)
	for _, scraper := range candidates {
		if scraper.matchesURL(url) {
			s := g.config.getScraper(scraper.scraperTypeConfig, client, g.txnManager, g.globalConf)
			ret, err := scrapeByUrl(ctx, url, s, ty)
			if err != nil {
				return nil, err
			}

			if ret != nil {
				return ret, nil
			}
		}
	}

	return nil, nil
}

func (g group) viaName(ctx context.Context, client *http.Client, name string, ty models.ScrapeContentType) ([]models.ScrapedContent, error) {
	switch ty {
	case models.ScrapeContentTypePerformer:
		if g.config.PerformerByName == nil {
			break
		}

		s := g.config.getScraper(*g.config.PerformerByName, client, g.txnManager, g.globalConf)
		performers, err := s.scrapePerformersByName(ctx, name)
		if err != nil {
			return nil, err
		}
		content := make([]models.ScrapedContent, len(performers))
		for i := range performers {
			content[i] = performers[i]
		}
		return content, nil
	case models.ScrapeContentTypeScene:
		if g.config.SceneByName == nil {
			break
		}

		s := g.config.getScraper(*g.config.SceneByName, client, g.txnManager, g.globalConf)
		scenes, err := s.scrapeScenesByName(ctx, name)
		if err != nil {
			return nil, err
		}
		content := make([]models.ScrapedContent, len(scenes))
		for i := range scenes {
			content[i] = scenes[i]
		}
		return content, nil
	}

	return nil, fmt.Errorf("%w: cannot load %v by name", ErrNotSupported, ty)
}

func (g group) supports(ty models.ScrapeContentType) bool {
	return g.config.supports(ty)
}

func (g group) supportsURL(url string, ty models.ScrapeContentType) bool {
	return g.config.matchesURL(url, ty)
}
