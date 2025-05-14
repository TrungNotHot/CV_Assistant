import QueryProvider from "@/providers/query-provider";
import type { Metadata, Viewport } from "next";
import { Inter } from "next/font/google";
import { NuqsAdapter } from "nuqs/adapters/next/app";
import "./globals.css";

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
    title: "Job Skills Explorer | Find In-Demand Skills for Your Career",
    description:
        "Discover the most in-demand skills for different job positions. Search by job title, experience level, and location to find the skills you need for your career.",
    keywords: [
        "job skills",
        "career skills",
        "skill analysis",
        "job requirements",
        "skill frequency",
        "technical skills",
        "soft skills",
        "career development",
        "job search",
        "resume analysis",
    ],
    authors: [{ name: "Job Skills Explorer Team" }],
    creator: "Job Skills Explorer",
    publisher: "Job Skills Explorer",
    formatDetection: {
        email: false,
        address: false,
        telephone: false,
    },
    openGraph: {
        type: "website",
        locale: "en_US",
        url: "http://localhost:3000",
        title: "Job Skills Explorer | Find In-Demand Skills for Your Career",
        description:
            "Discover the most in-demand skills for different job positions. Search by job title, experience level, and location.",
        siteName: "Job Skills Explorer",
        images: [
            {
                url: "/og-image.png",
                width: 1200,
                height: 630,
                alt: "Job Skills Explorer",
            },
        ],
    },
    twitter: {
        card: "summary_large_image",
        title: "Job Skills Explorer | Find In-Demand Skills for Your Career",
        description:
            "Discover the most in-demand skills for different job positions. Search by job title, experience level, and location.",
        images: ["/twitter-image.png"],
        creator: "@mrfour4",
    },
    robots: {
        index: true,
        follow: true,
        googleBot: {
            index: true,
            follow: true,
            "max-video-preview": -1,
            "max-image-preview": "large",
            "max-snippet": -1,
        },
    },
    alternates: {
        canonical: "http://localhost:3000",
    },
};

export const viewport: Viewport = {
    themeColor: [
        { media: "(prefers-color-scheme: light)", color: "#ffffff" },
        { media: "(prefers-color-scheme: dark)", color: "#111827" },
    ],
    width: "device-width",
    initialScale: 1,
    maximumScale: 1,
};
export default function RootLayout({
    children,
}: Readonly<{
    children: React.ReactNode;
}>) {
    return (
        <html lang="en">
            <head>
                <meta
                    name="apple-mobile-web-app-title"
                    content="Job Skills Explorer"
                />
            </head>
            <body className={inter.className}>
                <QueryProvider>
                    <NuqsAdapter>{children}</NuqsAdapter>
                </QueryProvider>
            </body>
        </html>
    );
}
