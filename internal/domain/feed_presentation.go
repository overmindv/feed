package domain

const MaterialCardTitlePrefix = "Опубликован материал: "

type CatalogCardPresentation struct {
	Kind        string
	TitlePrefix string
	HrefPrefix  string
}

func CatalogPresentation(aggregate string, activated bool) CatalogCardPresentation {
	name := aggregate
	switch aggregate {
	case "university":
		name = "университет"
	case "program":
		name = "программа"
	case "course":
		name = "курс"
	case "topic":
		name = "тема"
	}
	out := CatalogCardPresentation{HrefPrefix: "/" + aggregate + "s/"}
	if activated {
		out.Kind = aggregate + ".activated"
		out.TitlePrefix = "Активирован " + name + ": "
	} else {
		out.Kind = aggregate + ".created"
		out.TitlePrefix = "Новый " + name + ": "
	}

	return out
}
