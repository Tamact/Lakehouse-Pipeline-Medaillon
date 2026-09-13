# Export du flow NiFi (livrable obligatoire — section 6.2 du sujet)

Une fois le dataflow construit et validé dans l'UI NiFi :

1. Clic droit sur le Process Group **`FakeStoreAPI_Ingestion`**
2. **Download flow definition** → *without external controller services*
3. Enregistrer le fichier ici sous : **`FakeStoreAPI_Ingestion.json`**
4. `git add nifi/flow/FakeStoreAPI_Ingestion.json && git commit`

Ce fichier fait partie du code source à rendre. Il permet à l'enseignant de
réimporter le flow (`glisser un Process Group → Import from file`).
