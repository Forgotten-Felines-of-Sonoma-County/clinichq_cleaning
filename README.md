## Deploy to GCP Cloud Run

```bash
gcloud run deploy ff-tnr-dashboard --image gcr.io/project-beacon-459922/ff-tnr-dashboard --platform managed --region us-central1 --allow-unauthenticated --port 8080 --set-secrets SUPABASE_URL=SUPABASE_URL:latest,SUPABASE_KEY=SUPABASE_KEY:latest
```
