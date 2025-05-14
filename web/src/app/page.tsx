import { Header } from "@/modules/home/components/header";
import { PageContent } from "@/modules/home/components/page-content";

export default async function HomePage() {
    return (
        <main className="min-h-screen bg-gray-50">
            <div className="mx-auto max-w-5xl px-4 py-8">
                <Header />
                <PageContent />
            </div>
        </main>
    );
}
