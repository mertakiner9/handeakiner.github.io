// Recipe Service Layer
// Phase 2: Supabase integration

const SUPABASE_URL = "https://dobykurhhrfcflselhwe.supabase.co";
const SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImRvYnlrdXJoaHJmY2Zsc2VsaHdlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE1NzcxMTIsImV4cCI6MjA4NzE1MzExMn0.LkQDF9O_WzRzcNSpS2NO0YSjTeen_DIuc5WsSyPP3CQ";

class RecipeService {
    constructor() {
        this._realtimeSubscription = null;
    }

    async getAllRecipes() {
        const url = `${SUPABASE_URL}/rest/v1/recipes?select=*&order=published_date.desc&limit=1000`;
        const res = await fetch(url, {
            headers: {
                "apikey": SUPABASE_ANON_KEY,
                "Authorization": `Bearer ${SUPABASE_ANON_KEY}`
            }
        });

        if (!res.ok) {
            const err = await res.text();
            return { data: null, error: err };
        }

        const data = await res.json();
        return { data, error: null };
    }

    subscribeToChanges(callback) {
        // Supabase Realtime via supabase-js (loaded separately)
        if (!window.supabase) return null;

        this._realtimeSubscription = window.supabase
            .channel('recipes-changes')
            .on('postgres_changes', { event: '*', schema: 'public', table: 'recipes' }, callback)
            .subscribe();

        return this._realtimeSubscription;
    }

    unsubscribe() {
        if (this._realtimeSubscription) {
            this._realtimeSubscription.unsubscribe();
            this._realtimeSubscription = null;
        }
    }
}

window.recipeService = new RecipeService();
