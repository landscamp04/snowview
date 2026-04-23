import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";
import Navbar from "@/components/Navbar";

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "SnowView — Snow Conditions Intelligence",
  description:
    "Real-time snow conditions for ski resorts across California, Colorado, and Washington",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="dark">
      <body className={`${inter.className} bg-slate-950 text-slate-100 min-h-screen`}>
        <Navbar />
        {/* pt-14 offsets the fixed navbar height */}
        <main className="pt-14">{children}</main>
      </body>
    </html>
  );
}