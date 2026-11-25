package repository

const startFeedPreview = `
	INSERT INTO feed_rebuild_preview_runs (run_id, catalog_watermark, material_watermark)
	SELECT catalog.run_id, catalog.watermark, snapshot.watermark
	FROM feed_rebuild_catalog_runs AS catalog
	JOIN feed_rebuild_material_runs AS snapshot USING (run_id)
	JOIN feed_rebuild_material_replay AS replay USING (run_id)
	WHERE catalog.run_id = $1 AND catalog.through = catalog.watermark
		AND snapshot.complete AND replay.position = replay.target
	ON CONFLICT (run_id) DO NOTHING`

const feedPreviewInputConflict = `
	SELECT EXISTS (
		SELECT 1 FROM (
			SELECT aggregate_version, lag(aggregate_version) OVER (
				PARTITION BY aggregate_type, aggregate_id ORDER BY revision
			) AS previous
			FROM feed_rebuild_catalog_changes WHERE run_id = $1
		) AS versions WHERE aggregate_version <= previous
	) OR EXISTS (
		SELECT 1 FROM feed_rebuild_material_events WHERE run_id = $1
		GROUP BY event_id
		HAVING count(DISTINCT (material_id, version, revision_id, title, occurred_at)) > 1
	) OR EXISTS (
		SELECT 1 FROM feed_rebuild_material_events WHERE run_id = $1 AND revision_id IS NOT NULL
		GROUP BY material_id, version
		HAVING count(DISTINCT (revision_id, title, occurred_at)) > 1
	)`

const buildPreviewCatalogState = `
	INSERT INTO feed_rebuild_preview_catalog_state
		(run_id, resource_key, catalog_revision, aggregate_version, visible, fence_revision)
	SELECT DISTINCT ON (aggregate_type, aggregate_id)
		$1, aggregate_type || ':' || aggregate_id::text, revision, aggregate_version, title IS NOT NULL,
		max(CASE WHEN title IS NULL OR scope_reset THEN revision ELSE 0 END)
			OVER (PARTITION BY aggregate_type, aggregate_id)
	FROM feed_rebuild_catalog_changes
	WHERE run_id = $1
	ORDER BY aggregate_type, aggregate_id, revision DESC`

const buildPreviewCatalogCards = `
	WITH cards AS (
		SELECT DISTINCT ON (aggregate_type, aggregate_id, event_type)
			aggregate_type || ':' || aggregate_id::text AS resource_key,
			aggregate_id, event_type, actor_user_id, occurred_at, title, revision, ancestors
		FROM feed_rebuild_catalog_changes
		WHERE run_id = $1 AND title IS NOT NULL
		ORDER BY aggregate_type, aggregate_id, event_type, revision DESC
	)
	INSERT INTO feed_rebuild_preview_catalog_cards
		(run_id, event_key, kind, id, actor_user_id, occurred_at, title, text, href, catalog_revision, visibility_keys)
	SELECT $1, c.resource_key, rendering.kind, COALESCE(live.id, gen_random_uuid()), c.actor_user_id,
		c.occurred_at, rendering.title_prefix || c.title, c.title, rendering.href_prefix || c.aggregate_id::text,
		c.revision, array_prepend(c.resource_key, c.ancestors)
	FROM cards AS c
	JOIN jsonb_to_recordset($2::jsonb) AS rendering
		(event_type text, kind text, title_prefix text, href_prefix text) USING (event_type)
	LEFT JOIN feed_items AS live ON live.source = 'catalog' AND live.event_key = c.resource_key AND live.kind = rendering.kind`

const buildPreviewMaterial = `
	WITH stream AS (
		SELECT material_id, version, CASE WHEN NOT tombstone THEN revision_id END AS revision_id,
			CASE WHEN NOT tombstone THEN $2 || title END AS title, occurred_at
		FROM feed_rebuild_material_rows WHERE run_id = $1
		UNION ALL
		SELECT material_id, version, revision_id, title, occurred_at
		FROM feed_rebuild_material_events WHERE run_id = $1
	), newest AS (
		SELECT DISTINCT ON (material_id) material_id, version, revision_id, title, occurred_at
		FROM stream
		ORDER BY material_id, version DESC, (revision_id IS NULL) DESC, occurred_at DESC NULLS LAST
	)
	INSERT INTO feed_rebuild_preview_material_state (run_id, material_id, id, version, revision_id, title, occurred_at)
	SELECT $1, n.material_id, COALESCE(live.id, gen_random_uuid()), n.version, n.revision_id, n.title, n.occurred_at
	FROM newest AS n LEFT JOIN feed_material_state AS live USING (material_id)`

const compareFeedPreview = `
	WITH live AS (
		SELECT 'catalog' AS source, f.event_key, f.kind, f.id, f.occurred_at, f.title, f.text, f.href, f.actor_user_id
		FROM feed_items f WHERE f.source = 'catalog' AND ` + publicFeedFilter + `
		UNION ALL
		SELECT 'material', material_id::text, 'material.published', id, occurred_at, title, '',
			'/materials/' || material_id::text, NULL::uuid
		FROM feed_material_state WHERE ` + publicMaterialFilter + `
	), preview AS (
		SELECT 'catalog' AS source, c.event_key, c.kind, c.id, c.occurred_at, c.title, c.text, c.href, c.actor_user_id
		FROM feed_rebuild_preview_catalog_cards c WHERE c.run_id = $1 AND NOT EXISTS (
			SELECT 1 FROM unnest(c.visibility_keys) AS key(resource_key)
			JOIN feed_rebuild_preview_catalog_state v USING (resource_key)
			WHERE v.run_id = $1 AND v.fence_revision > 0 AND v.fence_revision > c.catalog_revision
		)
		UNION ALL
		SELECT 'material', material_id::text, 'material.published', id, occurred_at, title, '',
			'/materials/' || material_id::text, NULL::uuid
		FROM feed_rebuild_preview_material_state
		WHERE run_id = $1 AND revision_id IS NOT NULL AND occurred_at IS NOT NULL
	), card_diff AS (
		SELECT count(*) FILTER (WHERE p.source = 'catalog') AS catalog_cards,
			count(*) FILTER (WHERE p.source = 'material') AS material_cards,
			count(*) FILTER (WHERE p.id IS NULL) AS only_live,
			count(*) FILTER (WHERE live.id IS NULL) AS only_preview,
			count(*) FILTER (WHERE p.id IS NOT NULL AND live.id IS NOT NULL
				AND (p.id, p.occurred_at) IS DISTINCT FROM (live.id, live.occurred_at)) AS different_sort_keys,
			count(*) FILTER (WHERE p.id IS NOT NULL AND live.id IS NOT NULL
				AND (p.title, p.text, p.href, p.actor_user_id) IS DISTINCT FROM
					(live.title, live.text, live.href, live.actor_user_id)) AS different_fields
		FROM preview p FULL JOIN live USING (source, event_key, kind)
	), catalog_diff AS (
		SELECT count(*) AS conflicts
		FROM (SELECT * FROM feed_rebuild_preview_catalog_state WHERE run_id = $1) AS p
		FULL JOIN feed_catalog_visibility AS live USING (resource_key)
		WHERE (p.catalog_revision, p.aggregate_version, p.visible, p.fence_revision) IS DISTINCT FROM
			(live.catalog_revision, live.aggregate_version, live.visible, live.fence_revision)
	), material_diff AS (
		SELECT count(*) FILTER (WHERE p.revision_id IS NOT NULL AND p.occurred_at IS NULL) AS unresolved_dates,
			count(*) FILTER (WHERE (p.version, p.revision_id) IS DISTINCT FROM (live.aggregate_version, live.revision_id)) AS conflicts
		FROM (SELECT * FROM feed_rebuild_preview_material_state WHERE run_id = $1) AS p
		FULL JOIN feed_material_state AS live USING (material_id)
	)
	SELECT r.catalog_watermark, r.material_watermark,
		card_diff.catalog_cards, card_diff.material_cards, card_diff.only_live, card_diff.only_preview,
		card_diff.different_sort_keys, card_diff.different_fields, catalog_diff.conflicts, material_diff.conflicts,
		material_diff.unresolved_dates, ` + materialQuarantineHold + `
	FROM feed_rebuild_preview_runs r CROSS JOIN card_diff CROSS JOIN catalog_diff CROSS JOIN material_diff
	WHERE r.run_id = $1`
