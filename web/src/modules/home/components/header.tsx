import Link from "next/link";

export const Header = () => {
    return (
        <header>
            <h1 className="mb-2 text-center text-4xl font-bold">
                <Link href="/">Job Skills Explorer</Link>
            </h1>
            <p className="mb-8 text-center text-gray-500">
                Search job titles and discover the most in-demand skills and
                their frequencies
            </p>
        </header>
    );
};
