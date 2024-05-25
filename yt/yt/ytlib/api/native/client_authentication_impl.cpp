#include "client_impl.h"

#include <yt/yt/core/crypto/crypto.h>

#include <util/string/hex.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NCrypto;
using namespace NObjectClient;
using namespace NSecurityClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TClient::DoSetUserPassword(
    const TString& user,
    const TString& currentPasswordSha256,
    const TString& newPasswordSha256,
    const TSetUserPasswordOptions& options)
{
    DoValidateAuthenticationCommandPermissions(
        "Password change",
        user,
        currentPasswordSha256,
        options);

    constexpr int PasswordSaltLength = 16;
    auto newPasswordSaltBytes = GenerateCryptoStrongRandomString(PasswordSaltLength);
    auto newPasswordSalt = HexEncode(newPasswordSaltBytes.data(), newPasswordSaltBytes.size());

    auto hashedNewPassword = HashPasswordSha256(newPasswordSha256, newPasswordSalt);

    TMultisetAttributesNodeOptions multisetAttributesOptions;
    static_cast<TTimeoutOptions&>(multisetAttributesOptions) = options;

    auto rootClient = CreateRootClient();
    auto path = Format("//sys/users/%v/@", ToYPathLiteral(user));
    auto nodeFactory = GetEphemeralNodeFactory();
    auto attributes = nodeFactory->CreateMap();
    attributes->AddChild("hashed_password", ConvertToNode(hashedNewPassword));
    attributes->AddChild("password_salt", ConvertToNode(newPasswordSalt));
    WaitFor(rootClient->MultisetAttributesNode(
        path,
        attributes,
        multisetAttributesOptions))
        .ThrowOnError();

    YT_LOG_DEBUG("User password updated "
        "(User: %v, NewPasswordSha256: %v, HashedNewPassword: %v)",
        user,
        newPasswordSha256,
        hashedNewPassword);
}

TIssueTokenResult TClient::DoIssueToken(
    const TString& user,
    const TString& passwordSha256,
    const TIssueTokenOptions& options)
{
    DoValidateAuthenticationCommandPermissions(
        "Token issuance",
        user,
        passwordSha256,
        options);

    constexpr int TokenLength = 16;
    auto tokenBytes = GenerateCryptoStrongRandomString(TokenLength);
    auto token = to_lower(HexEncode(tokenBytes.data(), tokenBytes.size()));
    auto tokenHash = GetSha256HexDigestLowerCase(token);

    TCreateNodeOptions createOptions;
    static_cast<TTimeoutOptions&>(createOptions) = options;

    createOptions.Attributes = BuildAttributeDictionaryFluently()
        .Item("user").Value(user)
        .Finish();

    YT_LOG_DEBUG("Issuing new token for user (User: %v, TokenHash: %v)",
        user,
        tokenHash);

    auto rootClient = CreateRootClient();
    auto path = Format("//sys/cypress_tokens/%v", ToYPathLiteral(tokenHash));
    auto rspOrError = WaitFor(rootClient->CreateNode(
        path,
        EObjectType::MapNode,
        createOptions));

    if (!rspOrError.IsOK()) {
        YT_LOG_DEBUG(rspOrError, "Failed to issue new token for user "
            "(User: %v, TokenHash: %v)",
            user,
            tokenHash);
        auto error = TError("Failed to issue new token for user") << rspOrError;
        THROW_ERROR error;
    }

    YT_LOG_DEBUG("Issued new token for user (User: %v, TokenHash: %v)",
        user,
        tokenHash);

    return TIssueTokenResult{
        .Token = token,
    };
}

void TClient::DoRevokeToken(
    const TString& user,
    const TString& passwordSha256,
    const TString& tokenSha256,
    const TRevokeTokenOptions& options)
{
    auto rootClient = CreateRootClient();

    auto path = Format("//sys/cypress_tokens/%v", ToYPathLiteral(tokenSha256));

    TGetNodeOptions getOptions;
    static_cast<TTimeoutOptions&>(getOptions) = options;
    auto tokenUserOrError = WaitFor(rootClient->GetNode(Format("%v/@user", path), getOptions));
    if (!tokenUserOrError.IsOK()) {
        if (tokenUserOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            THROW_ERROR_EXCEPTION("Provided token is not recognized as a valid token for user %Qv", user);
        }

        YT_LOG_DEBUG(tokenUserOrError, "Failed to get user for token (TokenHash: %v)",
            tokenSha256);
        auto error = TError("Failed to get user for token")
            << tokenUserOrError;
        THROW_ERROR error;
    }

    auto tokenUser = ConvertTo<TString>(tokenUserOrError.Value());
    if (tokenUser != user) {
        THROW_ERROR_EXCEPTION("Provided token is not recognized as a valid token for user %Qv", user);
    }

    DoValidateAuthenticationCommandPermissions(
        "Token revokation",
        tokenUser,
        passwordSha256,
        options);

    TRemoveNodeOptions removeOptions;
    static_cast<TTimeoutOptions&>(removeOptions) = options;

    auto error = WaitFor(rootClient->RemoveNode(path, removeOptions));
    if (!error.IsOK()) {
        YT_LOG_DEBUG(error, "Failed to remove token (User: %v, TokenHash: %v)",
            tokenUser,
            tokenSha256);
        THROW_ERROR TError("Failed to remove token") << error;
    }

    YT_LOG_DEBUG("Token removed successfully (User: %v, TokenHash: %v)",
        tokenUser,
        tokenSha256);
}

TListUserTokensResult TClient::DoListUserTokens(
    const TString& user,
    const TString& passwordSha256,
    const TListUserTokensOptions& options)
{
    DoValidateAuthenticationCommandPermissions(
        "Tokens listing",
        user,
        passwordSha256,
        options);

    TListNodeOptions listOptions;
    static_cast<TTimeoutOptions&>(listOptions) = options;

    listOptions.Attributes = TAttributeFilter({"user"});

    auto rootClient = CreateRootClient();
    auto rspOrError = WaitFor(rootClient->ListNode("//sys/cypress_tokens", listOptions));
    if (!rspOrError.IsOK()) {
        YT_LOG_DEBUG(rspOrError, "Failed to list tokens");
        auto error = TError("Failed to list tokens") << rspOrError;
        THROW_ERROR error;
    }

    std::vector<TString> userTokens;

    auto tokens = ConvertTo<IListNodePtr>(rspOrError.Value());
    for (const auto& tokenNode : tokens->GetChildren()) {
        const auto& attributes = tokenNode->Attributes();
        auto userAttribute = attributes.Find<TString>("user");
        if (userAttribute == user) {
            userTokens.push_back(ConvertTo<TString>(tokenNode));
        }
    }

    return TListUserTokensResult{
        .Tokens = std::move(userTokens),
    };
}

void TClient::DoValidateAuthenticationCommandPermissions(
    TStringBuf action,
    const TString& user,
    const TString& passwordSha256,
    const TTimeoutOptions& options)
{
    constexpr TStringBuf HashedPasswordAttribute = "hashed_password";
    constexpr TStringBuf PasswordSaltAttribute = "password_salt";
    constexpr TStringBuf PasswordRevisionAttribute = "password_revision";

    bool canAdminister = false;
    if (Options_.User) {
        TCheckPermissionOptions checkPermissionOptions;
        static_cast<TTimeoutOptions&>(checkPermissionOptions) = options;

        auto rspOrError = WaitFor(CheckPermission(
            *Options_.User,
            Format("//sys/users/%v", ToYPathLiteral(user)),
            EPermission::Administer,
            checkPermissionOptions));
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Failed to check %Qlv permission for user", EPermission::Administer);

        canAdminister = (rspOrError.Value().Action == ESecurityAction::Allow);
    }

    if (!canAdminister) {
        if (Options_.User != user) {
            THROW_ERROR_EXCEPTION(
                "%v can be performed either by user theirselves "
                "or by a user having %Qlv permission on the user",
                action,
                EPermission::Administer)
                << TErrorAttribute("user", user)
                << TErrorAttribute("authenticated_user", Options_.User);
        }

        TGetNodeOptions getOptions;
        static_cast<TTimeoutOptions&>(getOptions) = options;

        getOptions.Attributes = std::vector<TString>({
            TString{HashedPasswordAttribute},
            TString{PasswordSaltAttribute},
            TString{PasswordRevisionAttribute},
        });

        auto path = Format("//sys/users/%v", ToYPathLiteral(user));
        auto rsp = WaitFor(GetNode(path, getOptions))
            .ValueOrThrow();
        auto rspNode = ConvertToNode(rsp);
        const auto& attributes = rspNode->Attributes();

        auto hashedPassword = attributes.Get<TString>(HashedPasswordAttribute);
        auto passwordSalt = attributes.Get<TString>(PasswordSaltAttribute);
        auto passwordRevision = attributes.Get<ui64>(PasswordRevisionAttribute);

        if (HashPasswordSha256(passwordSha256, passwordSalt) != hashedPassword) {
            THROW_ERROR_EXCEPTION("User provided invalid password")
                << TErrorAttribute("password_revision", passwordRevision);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
