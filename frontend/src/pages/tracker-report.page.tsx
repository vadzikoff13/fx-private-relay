import { useLocalization } from "@fluent/react";
import { NextPage } from "next";
import { useEffect, useState } from "react";
import styles from "./tracker-report.module.scss";
import logoType from "../../../static/images/fx-private-relay-logotype-dark.svg";
import logo from "../../../static/images/placeholder-logo.svg";
import { PageMetadata } from "../components/layout/PageMetadata";
import {
  HideIcon,
  InfoFilledIcon,
  InfoTriangleIcon,
} from "../components/Icons";
import Link from "next/link";
import { FaqAccordion } from "../components/landing/FaqAccordion";

// Paste this in your browser console to get a report URL:
// { let url = new URL("http://localhost:3000/trackerreport"); url.hash = JSON.stringify({ sender: "email@example.com", received_at: Date.now(), trackers: ["ads.facebook.com", "ads.googletagmanager.com"] }); url.href }
// This generates the following URL:
// http://localhost:3000/trackerreport/#{%22sender%22:%22email@example.com%22,%22received_at%22:1654866361357,%22trackers%22:[%22ads.facebook.com%22,%22ads.googletagmanager.com%22,%22hi.com%22]}

type ReportData = {
  sender: string;
  received_at: number;
  trackers: string[];
};

const TrackerReport: NextPage = () => {
  const { l10n } = useLocalization();
  const [reportData, setReportData] = useState<ReportData | null>();

  useEffect(() => {
    function updateReportData() {
      setReportData(parseHash(window.location.hash));
    }
    updateReportData();
    window.addEventListener("hashchange", updateReportData);
    return () => {
      window.removeEventListener("hashchange", updateReportData);
    };
  }, []);

  if (typeof reportData === "undefined") {
    return (
      <div className={styles.loading}>
        {l10n.getString("trackerreport-loading")}
      </div>
    );
  }
  if (reportData === null) {
    return (
      <div className={styles["load-error"]}>
        {l10n.getString("trackerreport-load-error")}
      </div>
    );
  }

  return (
    <>
      <PageMetadata />
      <div className={styles.wrapper}>
        <main className={styles["report-wrapper"]}>
          <div className={styles.report}>
            <b className={styles.logo}>
              <img
                src={logo.src}
                alt=""
                className={styles.logomark}
                width={42}
              />
              <img
                src={logoType.src}
                alt={l10n.getString("logo-alt")}
                className={styles.logotype}
                height={20}
              />
            </b>
            <h1>{l10n.getString("trackerreport-title")}</h1>
            <dl className={styles.meta}>
              <div className={styles.from}>
                <dt>{l10n.getString("trackerreport-meta-from-heading")}</dt>
                <dd>{reportData.sender}</dd>
              </div>
              <div className={styles["received_at"]}>
                <dt>
                  {l10n.getString("trackerreport-meta-received_at-heading")}
                </dt>
                <dd>{new Date(reportData.received_at).toLocaleString()}</dd>
              </div>
              <div className={styles.count}>
                <dt>{l10n.getString("trackerreport-meta-count-heading")}</dt>
                <dd>
                  {l10n.getString("trackerreport-trackers-value", {
                    count: reportData.trackers.length,
                  })}
                </dd>
              </div>
            </dl>
            <div className={styles.trackers}>
              <h2>{l10n.getString("trackerreport-trackers-heading")}</h2>
              <ul>
                {reportData.trackers.map((tracker) => (
                  <li key={tracker}>
                    <HideIcon alt="" />
                    {tracker}
                  </li>
                ))}
              </ul>
            </div>
            <div className={styles["confidentiality-notice"]}>
              <InfoFilledIcon alt="" />
              {l10n.getString("trackerreport-confidentiality-notice")}
            </div>
            <div className={styles.explainer}>
              <h2>
                {l10n.getString("trackerreport-removal-explainer-heading")}
              </h2>
              <p>{l10n.getString("trackerreport-removal-explainer-content")}</p>
              <hr aria-hidden="true" />
              <h2>
                {l10n.getString("trackerreport-trackers-explainer-heading")}
              </h2>
              <p>
                {l10n.getString(
                  "trackerreport-trackers-explainer-content-part1"
                )}
              </p>
              <p>
                {l10n.getString(
                  "trackerreport-trackers-explainer-content-part2"
                )}
              </p>
              <div className={styles["breakage-warning"]}>
                <InfoTriangleIcon alt="" />
                {l10n.getString("trackerreport-breakage-warning")}
              </div>
            </div>
          </div>
        </main>
        <section id="faq" className={styles["faq-wrapper"]}>
          <div className={styles.faq}>
            <div className={styles.lead}>
              <h2 className={styles.headline}>
                {l10n.getString("trackerreport-faq-heading")}
              </h2>
              <p>
                <Link href="/faq">
                  <a className={styles["read-more"]}>
                    {l10n.getString("trackerreport-faq-cta")}
                  </a>
                </Link>
              </p>
            </div>
            <div className={styles.entries}>
              <FaqAccordion
                entries={[
                  {
                    q: l10n.getString(
                      "faq-question-disable-trackerremoval-question"
                    ),
                    a: l10n.getString(
                      "faq-question-disable-trackerremoval-answer"
                    ),
                  },
                  {
                    q: l10n.getString(
                      "faq-question-bulk-trackerremoval-question"
                    ),
                    a: l10n.getString(
                      "faq-question-bulk-trackerremoval-answer"
                    ),
                  },
                  {
                    q: l10n.getString(
                      "faq-question-trackerremoval-breakage-question"
                    ),
                    a: l10n.getString(
                      "faq-question-trackerremoval-breakage-answer"
                    ),
                  },
                ]}
              />
            </div>
          </div>
        </section>
      </div>
    </>
  );
};

function parseHash(hash: string): ReportData | null {
  try {
    const data: unknown = JSON.parse(decodeURIComponent(hash.substring(1)));
    if (!containsReportData(data)) {
      return null;
    }
    return {
      sender: data.sender,
      received_at: data.received_at,
      trackers: data.trackers,
    };
  } catch (e) {
    return null;
  }
}

// This function does runtime type checking on user input,
// so we don't know its type at compile time yet:
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function containsReportData(parsed: any): parsed is ReportData {
  return (
    typeof parsed === "object" &&
    parsed !== null &&
    typeof parsed.sender === "string" &&
    Number.isInteger(parsed.received_at) &&
    Array.isArray(parsed.trackers) &&
    parsed.trackers.every((tracker: unknown) => typeof tracker === "string")
  );
}

export default TrackerReport;